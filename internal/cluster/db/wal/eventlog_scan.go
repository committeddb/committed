package wal

import (
	"context"
	"errors"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// scanRaw streams validated Entry bytes in the half-open Raft-index interval.
// It includes control and metadata entries and has no AppliedIndex filter:
// callers choose their bound for scrub selection, recovery, or verification.
// The backend holds a consistent view through all callbacks; no adapter lock is
// needed. Callbacks must not reenter the adapter or engine, and errors/cancellation
// can follow an already delivered prefix. Selection callers retain their own
// adapter lock when coordinating the scan with other operations.
func (l *eventLogAdapter) scanRaw(ctx context.Context, bounds eventlog.Coverage, visit func(uint64, []byte) error) error {
	if visit == nil {
		return eventlog.ErrInvalid
	}
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
