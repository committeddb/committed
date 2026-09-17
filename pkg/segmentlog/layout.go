package segmentlog

import (
	"context"
	"iter"
	"sort"
)

// layout is the managed log's metadata boundary. The Log mutex serializes its
// use across preparation, publication and file retirement. head never includes
// historical ranges; ranges yields only overlapping references. Current is a
// diagnostic full snapshot, not an operation used by the managed hot path.
type layout interface {
	head() (Catalog, error)
	ranges(Coverage) iter.Seq2[SegmentRef, error]
	Current() (Catalog, error)
	preflight() error
	publishRollover(*preparedRollover) error
	publishRewrite(uint64, uint64, []SegmentRef, *TailRef) error
	reclaim(*Log, context.Context) (ReclaimResult, error)
	reclaimOrphans(*Log, context.Context) (ReclaimResult, error)
	Close() error
}

func catalogHead(c Catalog) Catalog { c.Segments = nil; return cloneCatalog(c) }

func (s *CatalogStore) head() (Catalog, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return catalogHead(s.current), s.poison
}

func (s *CatalogStore) Close() error { return nil }

func (s *CatalogStore) reclaimOrphans(l *Log, ctx context.Context) (ReclaimResult, error) {
	return s.reclaim(l, ctx)
}

func (s *CatalogStore) ranges(bounds Coverage) iter.Seq2[SegmentRef, error] {
	return func(yield func(SegmentRef, error) bool) {
		c, err := s.Current()
		if err != nil {
			yield(SegmentRef{}, err)
			return
		}
		first := sort.Search(len(c.Segments), func(i int) bool { return c.Segments[i].Coverage.End > bounds.Start })
		for _, ref := range c.Segments[first:] {
			if ref.Coverage.Start >= bounds.End {
				return
			}
			if !yield(ref, nil) {
				return
			}
		}
	}
}

func (s *CatalogStore) preflight() error {
	c, err := s.Current()
	if err != nil {
		return err
	}
	return verifyCatalogFiles(s.path, c)
}

func (s *CatalogStore) publishRewrite(expected, generation uint64, changed []SegmentRef, active *TailRef) error {
	c, err := s.Current()
	if err != nil {
		return err
	}
	if c.Revision != expected {
		return ErrCatalogConflict
	}
	for _, ref := range changed {
		i := sort.Search(len(c.Segments), func(i int) bool { return c.Segments[i].Coverage.Start >= ref.Coverage.Start })
		if i == len(c.Segments) || c.Segments[i].Coverage != ref.Coverage {
			return ErrInvalid
		}
		c.Segments[i] = ref
	}
	c.Revision++
	c.Generation = generation
	c.Active = active
	return s.Publish(expected, c)
}
