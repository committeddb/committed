package segmentlog

// InspectCatalog returns a detached snapshot for diagnostics. It materializes
// every range, so its time and memory grow with history size. It holds the log
// mutex while reading metadata, but does not pin files after returning. Normal
// reads and mutations use targeted metadata queries instead.
func (l *Log) InspectCatalog() (Catalog, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.usable(); err != nil {
		return Catalog{}, err
	}
	return l.catalog.Current()
}

// Generation reads the selected logical rewrite generation from catalog header
// metadata without materializing the range list. Appends and reclamation do not
// advance it; poisoned handles must be reopened before consulting this value.
func (l *Log) Generation() (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.usable(); err != nil {
		return 0, err
	}
	c, err := l.catalog.head()
	return c.Generation, err
}
