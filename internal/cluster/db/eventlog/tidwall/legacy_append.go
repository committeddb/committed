package tidwall

import (
	"sync"

	wal "github.com/tidwall/wal"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// LegacyCodec preserves the application's existing entry envelope. Encode must
// retain the record ID; Decode extracts it from an existing physical entry.
// Callbacks must not reenter the appender and must not mutate their input.
type LegacyCodec struct {
	Encode func(eventlog.Record) ([]byte, error)
	Decode func([]byte) (eventlog.Record, error)
}

// LegacyAppender writes logical records to an already-open production tidwall
// log. It assigns dense physical sequences and preserves the supplied envelope;
// it does not create CURRENT or the experimental generation directory format.
// The caller owns the native handle and must exclude handle swaps and external
// writes during Append. Rewrap after replacing the handle.
type LegacyAppender struct {
	mu             sync.Mutex
	log            *wal.Log
	codec          LegacyCodec
	sequence, last uint64
	valid          bool
}

var _ eventlog.Appender = (*LegacyAppender)(nil)

func NewLegacyAppender(log *wal.Log, codec LegacyCodec) *LegacyAppender {
	return &LegacyAppender{log: log, codec: codec}
}

func (l *LegacyAppender) Append(records []eventlog.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.log == nil || l.codec.Encode == nil || l.codec.Decode == nil {
		return eventlog.ErrInvalid
	}
	sequence, err := l.log.LastIndex()
	if err != nil {
		return err
	}
	last := l.last
	if !l.valid || sequence != l.sequence {
		if sequence > 0 {
			raw, err := l.log.Read(sequence)
			if err != nil {
				return err
			}
			record, err := l.codec.Decode(raw)
			if err != nil {
				return err
			}
			last = record.ID
		}
	}
	if uint64(len(records)) > ^uint64(0)-sequence {
		return eventlog.ErrInvalid
	}
	batch := new(wal.Batch)
	next := sequence
	for i, r := range records {
		if r.ID == ^uint64(0) || ((sequence > 0 || i > 0) && r.ID <= last) {
			return eventlog.ErrInvalid
		}
		raw, err := l.codec.Encode(r)
		if err != nil {
			return err
		}
		next++
		batch.Write(next, raw)
		last = r.ID
	}
	if len(records) == 0 {
		return nil
	}
	if err := l.log.WriteBatch(batch); err != nil {
		l.valid = false
		return err
	}
	l.sequence, l.last, l.valid = next, last, true
	return nil
}
