package wal

import (
	"errors"

	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// scanEventEntries visits the inclusive logical prefix through bound. It includes
// metadata and control entries without an applied watermark. The publication
// read lock keeps one generation throughout selection; appends can continue.
// Callbacks must not reenter Storage. The cursor and its decoded entry are local
// to this scan and are released on every exit.
func (s *Storage) scanEventEntries(bound uint64, visit func(*pb.Entry) error) error {
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	cursor := s.eventLog.entries.NewEntryCursor(0)
	defer func() { _ = cursor.Close() }()
	return scanEntryPrefix(cursor, bound, visit)
}

// scanEntryPrefix is independent of the physical backend. The owner supplies
// the read lifetime and closes the cursor.
func scanEntryPrefix(cursor entryCursor, bound uint64, visit func(*pb.Entry) error) error {
	if visit == nil {
		return eventlog.ErrInvalid
	}
	if err := cursor.SeekGE(0); err != nil {
		return err
	}
	for {
		entry, err := cursor.Current()
		if errors.Is(err, eventlog.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if entry.GetIndex() > bound {
			return nil
		}
		if err := visit(entry); err != nil {
			return err
		}
		cursor.Advance()
		if entry.GetIndex() == bound {
			return nil
		}
	}
}
