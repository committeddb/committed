package segmentlog

import (
	"context"
	"errors"
	"iter"

	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

// ScanReverse visits at most limit surviving records, newest first, under one
// log lock. The count includes callbacks returning false or an error. False
// stops successfully. Callbacks must not reenter the log. Payloads are owned by
// the caller. Cancellation is checked between records and ranges.
//
// Cached ranges and indexed blocks are read backward. An unindexed append file
// is scanned forward once, retaining at most limit suffix records. Thus limit
// bounds delivery and suffix retention, not physical I/O within an append file.
// Older ranges are not opened after the limit or callback stops the scan.
func (l *Log) ScanReverse(ctx context.Context, limit int, visit func(Record) (bool, error)) (count int, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.usable(); err != nil {
		return 0, err
	}
	if ctx == nil || limit < 0 || visit == nil {
		return 0, ErrInvalid
	}
	if err := ctx.Err(); err != nil || limit == 0 {
		return 0, err
	}
	c, err := l.catalog.head()
	if err != nil {
		return 0, err
	}
	stopped := false
	deliver := func(r Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		count++
		more, err := visit(r)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !more || count == limit {
			stopped = true
			return errStopScan
		}
		return nil
	}
	defer func() {
		if stopped && err == errStopScan {
			err = nil
		}
	}()
	if l.resident != nil {
		err = reverseCached(ctx, l.resident.view(c.Active.Start), deliver)
	} else {
		state, e := l.tail.State()
		if e != nil {
			return count, e
		}
		err = reverseSuffix(ctx, tailRecords(l.file, state.End), limit, deliver)
	}
	if err != nil {
		return count, err
	}
	// Query the range containing end-1, then move to its start. This uses
	// catalog seeks rather than materializing the full range list.
	for end := c.Active.Start; end > c.Start; {
		found := false
		for ref, e := range l.catalog.ranges(Coverage{end - 1, end}) {
			if e != nil {
				return count, e
			}
			found = true
			end = ref.Coverage.Start
			if e := ctx.Err(); e != nil {
				return count, e
			}
			if ref.Count == 0 {
				break
			}
			source, release, e := l.acquireRange(ref)
			if e != nil {
				return count, e
			}
			e = reverseRange(ctx, source, limit-count, deliver)
			closeErr := release()
			if e != nil || closeErr != nil {
				if stopped && e == errStopScan {
					return count, closeErr
				}
				return count, errors.Join(e, closeErr)
			}
			break
		}
		if !found {
			return count, ErrCorrupt
		}
	}
	return count, ctx.Err()
}

func reverseRange(ctx context.Context, source rangeSource, limit int, visit func(Record) error) error {
	switch s := source.(type) {
	case *cachedSegment:
		return reverseCached(ctx, s, visit)
	case *Segment:
		var decoder format.Decoder
		defer decoder.Close()
		var records []Record
		var compressed []byte
		for i := len(s.blocks) - 1; i >= 0; i-- {
			if err := ctx.Err(); err != nil {
				return err
			}
			var err error
			records, err = s.readBlock(s.blocks[i], &decoder, records, &compressed)
			if err != nil {
				return err
			}
			for j := len(records) - 1; j >= 0; j-- {
				if err := visit(records[j]); err != nil {
					return err
				}
			}
		}
		return nil
	default:
		return reverseSuffix(ctx, source.Records(), limit, visit)
	}
}

func reverseCached(ctx context.Context, s *cachedSegment, visit func(Record) error) error {
	for i := len(s.records) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(s.record(i)); err != nil {
			return err
		}
	}
	return nil
}

func reverseSuffix(ctx context.Context, records iter.Seq2[Record, error], limit int, visit func(Record) error) error {
	var suffix []Record
	next := 0
	for r, err := range records {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(suffix) < limit {
			suffix = append(suffix, Record{})
		}
		// Copy borrowed iterator bytes into the ring, reusing each slot's
		// buffer. No callback sees a slot until forward scanning is complete.
		suffix[next] = Record{ID: r.ID, Payload: append(suffix[next].Payload[:0], r.Payload...)}
		next = (next + 1) % limit
	}
	for range len(suffix) {
		next = (next + len(suffix) - 1) % len(suffix)
		if err := visit(suffix[next]); err != nil {
			return err
		}
	}
	return nil
}
