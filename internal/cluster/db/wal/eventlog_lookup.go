package wal

// readRaw and seekRaw retain ErrNotFound for absent/erased indexes. Corruption
// must never be interpreted as a gap or EOF. Returned payloads belong to the caller.
// Each lookup uses one atomic backend operation, so it needs no adapter read lock.
// Separate calls may observe different rewrite generations.
func (l *eventLogAdapter) readRaw(index uint64) ([]byte, error) {
	r, err := l.log.Read(index)
	return checkedEventEntry(r, err)
}

func (l *eventLogAdapter) seekRaw(index uint64) (uint64, []byte, error) {
	r, err := l.log.Seek(index)
	raw, err := checkedEventEntry(r, err)
	if err != nil {
		return 0, nil, err
	}
	return r.ID, raw, nil
}
