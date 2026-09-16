package wal

import (
	"context"
	"errors"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// scanRaw streams validated Entry bytes in the half-open Raft-index interval.
// It includes control and metadata entries and has no AppliedIndex filter:
// callers choose their bound for scrub selection, recovery, or verification.
// It holds the adapter and engine view through the callback. Callbacks must not
// reenter either, and errors/cancellation can follow an already delivered prefix.
func (l *eventLogAdapter) scanRaw(ctx context.Context, bounds eventlog.Coverage, visit func(uint64, []byte) error) error {
	if visit == nil {
		return eventlog.ErrInvalid
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.scanRawLocked(ctx, bounds, visit)
}

// scanRawLocked requires the adapter read or write lock.
func (l *eventLogAdapter) scanRawLocked(ctx context.Context, bounds eventlog.Coverage, visit func(uint64, []byte) error) error {
	err := l.log.Scan(ctx, bounds, func(r eventlog.Record) error {
		raw, err := checkedEventEntry(r, nil)
		if err != nil {
			return err
		}
		return visit(r.ID, raw)
	})
	if errors.Is(err, eventlog.ErrCorrupt) {
		return errors.Join(ErrCorruptEntry, err)
	}
	return err
}
