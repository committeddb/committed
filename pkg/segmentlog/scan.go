package segmentlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
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
// Memory is bounded by decoded blocks/groups unless the caller retains payloads.
// Unvisited files/blocks are not verified; use full verification separately.
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
	c, err := l.catalog.Current()
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
	first := sort.Search(len(c.Segments), func(i int) bool { return c.Segments[i].Coverage.End > bounds.Start })
	for _, ref := range c.Segments[first:] {
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
	if c.Active.Start >= bounds.End {
		return ctx.Err()
	}
	state, err := l.tail.State()
	if err != nil {
		return err
	}
	reachedEnd := false
	_, err = scanTail(l.file, state.End, c.Active.Checkpoint, func(r Record) error {
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
	f, err := os.Open(filepath.Join(l.path, ref.File))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	segment, err := OpenSegment(f, info.Size())
	if err != nil {
		return err
	}
	if segment.Coverage() != ref.Coverage || segment.Count() != ref.Count {
		return ErrCorrupt
	}
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
