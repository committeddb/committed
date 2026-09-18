package segmentlog

// LastAppended returns the highest original appended ID, including erased
// records. ok is false only when no range or tail records establish any append
// history; a nonzero creation Start alone is not evidence of an append. ID zero
// is valid when ok is true. This is append progress, not a surviving-record lookup.
//
// Only a usable handle reports progress. After an I/O failure Close and reopen
// first: recovery may discover complete groups from an unacknowledged append.
func (l *Log) LastAppended() (id uint64, ok bool, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.usable(); err != nil {
		return 0, false, err
	}
	state, err := l.tail.State()
	if err != nil {
		return 0, false, l.fail(err)
	}
	if state.HasRecords {
		return state.Last, true, nil
	}
	c, err := l.catalog.head()
	if err != nil {
		return 0, false, l.fail(err)
	}
	if c.Active.Start > c.Start {
		// Rotation fixes coverage at the original last ID + 1. It remains valid
		// after every record in that range is erased, even with an empty new tail.
		return c.Active.Start - 1, true, nil
	}
	return 0, false, nil
}
