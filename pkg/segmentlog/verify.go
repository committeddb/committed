package segmentlog

import "context"

// Verify checks all selected range contents and digests, then the active tail.
// It streams metadata under the Log mutex. This is an explicit full-history
// operation, including for bbolt logs whose Open checks only metadata boundaries
// and the active tail. Cancellation is checked between files.
func (l *Log) Verify(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.usable(); err != nil {
		return err
	}
	if ctx == nil {
		return ErrInvalid
	}
	c, err := l.catalog.head()
	if err != nil {
		return err
	}
	for ref, e := range l.catalog.ranges(Coverage{c.Start, c.Active.Start}) {
		if e != nil {
			return e
		}
		if e = ctx.Err(); e != nil {
			return e
		}
		if e = verifyCatalogFiles(l.path, Catalog{Segments: []SegmentRef{ref}}); e != nil {
			return e
		}
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	return verifyCatalogFiles(l.path, Catalog{Active: c.Active})
}
