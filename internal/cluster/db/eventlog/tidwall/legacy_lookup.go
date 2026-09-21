package tidwall

import (
	"bytes"
	"fmt"

	wal "github.com/tidwall/wal"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// LegacyLookup resolves stable IDs in an already-open production tidwall log.
// The owner must exclude truncation, replacement, and close throughout Read.
// Decode must be safe for concurrent calls and must not mutate its input.
// As with LegacyAppender, the caller supplies the existing envelope codec.
type LegacyLookup struct {
	log    *wal.Log
	decode func([]byte) (eventlog.Record, error)
}

var _ eventlog.Lookup = (*LegacyLookup)(nil)

func NewLegacyLookup(log *wal.Log, decode func([]byte) (eventlog.Record, error)) *LegacyLookup {
	return &LegacyLookup{log: log, decode: decode}
}

func (l *LegacyLookup) Read(id uint64) (eventlog.Record, error) {
	if l.log == nil || l.decode == nil {
		return eventlog.Record{}, eventlog.ErrInvalid
	}
	first, err := l.log.FirstIndex()
	if err != nil {
		return eventlog.Record{}, err
	}
	last, err := l.log.LastIndex()
	if err != nil {
		return eventlog.Record{}, err
	}
	if first == 0 || last < first {
		return eventlog.Record{}, eventlog.ErrNotFound
	}
	for lo, hi := first, last; lo <= hi; {
		mid := lo + (hi-lo)/2
		raw, err := l.log.Read(mid)
		if err != nil {
			return eventlog.Record{}, fmt.Errorf("event log read seq %d: %w", mid, err)
		}
		record, err := l.decode(raw)
		if err != nil {
			return eventlog.Record{}, err
		}
		switch {
		case record.ID == id:
			// The codec may return a view into the native read buffer.
			record.Payload = bytes.Clone(record.Payload)
			return record, nil
		case record.ID < id:
			if mid == last {
				return eventlog.Record{}, eventlog.ErrNotFound
			}
			lo = mid + 1
		default:
			if mid == first {
				return eventlog.Record{}, eventlog.ErrNotFound
			}
			hi = mid - 1
		}
	}
	return eventlog.Record{}, eventlog.ErrNotFound
}
