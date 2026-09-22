package wal

import (
	"errors"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// eventBoundsLocked recovers the first survivor and original append progress.
// Callers hold eventMu (or own startup exclusively). Erasure can leave no
// survivors without resetting append progress. Neither bound is published here.
func (s *Storage) eventBoundsLocked() (first, last uint64, err error) {
	last, ok, err := s.eventLog.entries.LastAppended()
	if err != nil {
		return 0, 0, err
	}
	if !ok {
		return 0, 0, nil
	}
	cursor := s.eventLog.entries.NewEntryCursor(0)
	defer func() { _ = cursor.Close() }()
	head, err := cursor.Current()
	if errors.Is(err, eventlog.ErrNotFound) {
		return 0, last, nil
	}
	if err != nil {
		return 0, 0, err
	}
	return head.GetIndex(), last, nil
}
