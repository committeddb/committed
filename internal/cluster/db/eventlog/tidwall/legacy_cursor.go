package tidwall

import (
	"errors"
	"fmt"
	"sync"

	wal "github.com/tidwall/wal"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// LegacyCursor translates logical seeks into dense native sequences. The owner
// must exclude truncation, replacement, and close during Seek, and create a new
// cursor after any change that renumbers sequences. Appends need no invalidation.
// Decode must return a value that remains valid after subsequent native reads.
// The application owns decoding; positioning carries its result without copying
// or interpreting it. The cursor retains hints, not decoded values.
type LegacyCursor[T any] struct {
	mu                         sync.Mutex
	log                        *wal.Log
	decode                     func([]byte) (uint64, T, error)
	sequence, requested, found uint64
	valid, closed              bool
}

func NewLegacyCursor[T any](log *wal.Log, decode func([]byte) (uint64, T, error)) *LegacyCursor[T] {
	return &LegacyCursor[T]{log: log, decode: decode}
}

func (c *LegacyCursor[T]) at(sequence uint64) (uint64, T, error) {
	raw, err := c.log.Read(sequence)
	if err != nil {
		var zero T
		return 0, zero, fmt.Errorf("event log read seq %d: %w", sequence, err)
	}
	return c.decode(raw)
}

func (c *LegacyCursor[T]) Seek(id uint64) (T, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var zero T
	if c.closed {
		return zero, eventlog.ErrClosed
	}
	if c.log == nil || c.decode == nil {
		return zero, eventlog.ErrInvalid
	}
	last, err := c.log.LastIndex()
	if err != nil {
		return zero, err
	}
	seq := c.sequence
	direct := c.valid && id >= c.requested && id <= c.found
	if c.valid && c.found != ^uint64(0) && id == c.found+1 {
		if seq == last {
			return zero, eventlog.ErrNotFound
		}
		seq++
		direct = true
	}
	var record T
	var found uint64
	if direct {
		found, record, err = c.at(seq)
	} else {
		seq, found, record, err = c.seek(id, last)
	}
	if err != nil {
		return zero, err
	}
	c.sequence, c.requested, c.found, c.valid = seq, id, found, true
	return record, nil
}

func (c *LegacyCursor[T]) seek(id, last uint64) (uint64, uint64, T, error) {
	var zero T
	first, err := c.log.FirstIndex()
	if err != nil {
		return 0, 0, zero, err
	}
	if first == 0 || last < first {
		return 0, 0, zero, eventlog.ErrNotFound
	}
	// The beginning of the logical ID space is the physical head. Prefix
	// scans need not probe (or decode) later records to find it.
	if id == 0 {
		index, value, err := c.at(first)
		return first, index, value, err
	}
	// Lower-bound search. Retain the best decoded candidate so the selected
	// record is not read again after the search.
	var candidate T
	var sequence, found uint64
	for lo, hi := first, last; lo <= hi; {
		mid := lo + (hi-lo)/2
		index, record, err := c.at(mid)
		if err != nil {
			return 0, 0, zero, err
		}
		if index == id {
			return mid, index, record, nil
		}
		if index < id {
			if mid == last {
				break
			}
			lo = mid + 1
		} else {
			sequence, found, candidate = mid, index, record
			if mid == first {
				break
			}
			hi = mid - 1
		}
	}
	if sequence == 0 {
		return 0, 0, zero, eventlog.ErrNotFound
	}
	return sequence, found, candidate, nil
}

func (c *LegacyCursor[T]) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed, c.valid = true, false
	c.log, c.decode = nil, nil
	return nil
}

// SequenceFor resolves the native peer-transfer starting sequence. Unlike Seek,
// absence returns the sequence immediately after the current tail (1 when empty).
// This physical position is only for the native-format transfer protocol; it is
// not an application checkpoint. The owner holds its read lifetime as for Seek.
func (c *LegacyCursor[T]) SequenceFor(id uint64) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, eventlog.ErrClosed
	}
	if c.log == nil || c.decode == nil {
		return 0, eventlog.ErrInvalid
	}
	last, err := c.log.LastIndex()
	if err != nil {
		return 0, err
	}
	// A head request has never required decoding any record in the native
	// transfer protocol; payload validation happens when records are served.
	if id == 0 {
		first, err := c.log.FirstIndex()
		if err != nil {
			return 0, err
		}
		if first != 0 {
			return first, nil
		}
	}
	sequence, _, _, err := c.seek(id, last)
	if errors.Is(err, eventlog.ErrNotFound) {
		if last == ^uint64(0) {
			return 0, eventlog.ErrInvalid
		}
		return last + 1, nil
	}
	return sequence, err
}
