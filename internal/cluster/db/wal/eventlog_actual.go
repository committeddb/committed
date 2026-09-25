package wal

import (
	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// actualAt performs an experimental exact lookup without changing reader cursors.
// Like Storage.ActualAt, it returns all proposal entities, including metadata;
// streaming-reader filtering and unknown-system-type skipping do not apply here.
// Missing/erased indexes and control/no-op entries return ErrActualNotFound.
// It additionally gates proposal decoding on the supplied applied watermark:
// a durable but unapplied entry is temporarily ErrActualNotFound. Production
// Storage.ActualAt is unchanged. Resolver and applied must not reenter the adapter.
func (l *eventLogAdapter) actualAt(index uint64, resolver cluster.TypeResolver, applied func() uint64) (*cluster.Actual, error) {
	if resolver == nil || applied == nil {
		return nil, eventlog.ErrInvalid
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	cursor := newEventEntryCursor(l.log.NewCursor(), index)
	defer func() { _ = cursor.Close() }()
	entry, err := exactEntry(cursor, index)
	if err != nil {
		return nil, err
	}
	if index > applied() {
		return nil, ErrActualNotFound
	}
	return actualFromEntry(entry, resolver)
}
