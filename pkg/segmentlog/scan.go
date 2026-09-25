package segmentlog

import (
	"context"
	"errors"
)

// Scan visits surviving records in [bounds.Start, bounds.End), in increasing ID
// order, under one managed log lock. Empty intervals and intervals containing no
// survivors succeed without delivery. Reversed intervals and nil callbacks are
// invalid. Returning an error stops immediately; earlier deliveries remain valid.
//
// The callback must not reenter this Log. Appends, rewrites, and reclamation wait
// until Scan returns; this is a synchronous view, not a persistent reader pin.
// Cancellation is checked between records, not during I/O or the callback.
// Sealed scans use block indexes; the unindexed tail prefix is scanned once.
// Without caching, memory is bounded by decoded blocks/groups unless the caller
// retains payloads. An enabled cache materializes whole closed ranges
// on a miss. Cache hits do not inspect disk; use Verify for disk verification.
func (l *Log) Scan(ctx context.Context, bounds Coverage, visit func(Record) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.usable(); err != nil {
		return err
	}
	if ctx == nil || visit == nil || bounds.End < bounds.Start {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if bounds.Start == bounds.End {
		return nil
	}
	c, err := l.catalog.head()
	if err != nil {
		return err
	}
	deliver := func(r Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(r); err != nil {
			return err
		}
		return ctx.Err()
	}
	// The header already identifies the tail; avoid another metadata transaction
	// when the requested interval cannot overlap any closed range.
	if bounds.Start < c.Active.Start {
		for ref, err := range l.catalog.ranges(bounds) {
			if err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if ref.Coverage.Start >= bounds.End {
				return nil
			}
			if ref.Count == 0 {
				continue
			}
			if err := l.scanSegment(ref, bounds, deliver); err != nil {
				return err
			}
		}
	}
	if c.Active.Start >= bounds.End {
		return ctx.Err()
	}
	if l.resident != nil {
		for r, err := range l.resident.view(c.Active.Start).recordsIn(bounds) {
			if err != nil {
				return err
			}
			if err := deliver(r); err != nil {
				return err
			}
		}
		return ctx.Err()
	}
	state, err := l.tail.State()
	if err != nil {
		return err
	}
	reachedEnd := false
	_, err = scanManagedTail(l.file, state.End, *c.Active, func(r Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if r.ID >= bounds.End {
			reachedEnd = true
			return errStopScan
		}
		if r.ID < bounds.Start {
			return nil
		}
		return deliver(r)
	})
	if reachedEnd && errors.Is(err, errStopScan) {
		return ctx.Err()
	}
	if err != nil {
		return err
	}
	return ctx.Err()
}

func (l *Log) scanSegment(ref SegmentRef, bounds Coverage, visit func(Record) error) (err error) {
	segment, release, err := l.acquireRange(ref)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, release()) }()
	for record, err := range segment.recordsIn(bounds) {
		if err != nil {
			return err
		}
		if err := visit(record); err != nil {
			return err
		}
	}
	return nil
}
