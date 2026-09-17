package segmentlog

// publishRollover consumes preparation from the exclusively owned segment
// storage. Unlike public Publish, it does not accept an arbitrary caller-built
// layout or establish file durability. Recovery never uses this entry point.
func (s *CatalogStore) publishRollover(p *preparedRollover) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poison != nil {
		return s.poison
	}
	if p == nil || p.consumed || p.file == nil || p.tail == nil {
		return ErrInvalid
	}
	p.consumed = true
	c := s.current
	if p.path != s.path || p.history != c.History || p.revision != c.Revision || c.Active == nil || p.source != c.Active.File {
		return ErrCatalogConflict
	}
	if c.Revision == ^uint64(0) || p.closed.Coverage.Start != c.Active.Start || p.active.Start != p.closed.Coverage.End {
		return ErrInvalid
	}
	next := cloneCatalog(c)
	next.Revision++
	next.Segments = append(next.Segments, p.closed)
	active := p.active
	next.Active = &active
	b, err := encodeCatalog(next)
	if err != nil {
		return err
	}
	return s.publishMetadata(next, b, false)
}
