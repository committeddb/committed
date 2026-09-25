package tidwall

import "errors"

// Reset selects a new empty directory using the same CURRENT publication as a
// rewrite. It retains configuration and generation but clears append history.
// Unselected directories are left for Reclaim.
func (l *Log) Reset() (err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.usable(); err != nil {
		return err
	}
	next := l.state
	next.Last, next.Count, next.HasAppends = 0, 0, false
	name, replacement, err := newData(l.path, next, false)
	if err != nil {
		return l.fail(err)
	}
	defer func() {
		if replacement != nil {
			err = errors.Join(err, replacement.Close())
		}
	}()
	if err := l.syncData(replacement, name); err != nil {
		return l.fail(err)
	}
	next.Directory = name
	if err := l.publish(next); err != nil {
		return l.fail(err)
	}
	old := l.data
	l.data, l.state, l.last, l.has = replacement, next, 0, false
	replacement = nil
	if err := old.Close(); err != nil {
		return l.fail(err)
	}
	return nil
}
