package wal

import (
	"errors"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// legacyEventBoundsLocked reads logical boundaries through the native positioner.
// Callers hold eventMu (or own startup exclusively). It does not publish partial
// progress when either boundary is corrupt. Native scrubbing preserves the tail;
// these bounds are surviving entries, not an erased-history frontier.
func (s *Storage) legacyEventBoundsLocked() (first, last uint64, err error) {
	positioner := newLegacyPositioner(s)
	defer func() { _ = positioner.Close() }()
	tail, err := positioner.Last()
	if errors.Is(err, eventlog.ErrNotFound) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	head, err := positioner.Seek(0)
	if err != nil {
		return 0, 0, err
	}
	return head.GetIndex(), tail.GetIndex(), nil
}
