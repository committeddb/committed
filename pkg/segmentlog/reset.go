package segmentlog

import (
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
)

// Reset atomically selects an empty history, clearing even erased append
// progress. It preserves the configured start, encoding and generation. Existing
// cursors discard their previous positions. Retired files remain for Reclaim.
// On I/O failure, close/reopen before deciding whether reset published.
func (l *Log) Reset() (err error) {
	l.maintenanceMu.Lock()
	defer l.maintenanceMu.Unlock()
	l.mutationMu.Lock()
	defer l.mutationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.usable(); err != nil {
		return err
	}
	c, err := l.catalog.head()
	if err != nil {
		return l.fail(err)
	}
	if c.Revision == ^uint64(0) {
		return ErrInvalid
	}
	if _, err = rand.Read(c.History[:]); err != nil {
		return err
	}
	name, err := uniqueName("tail", c.Start, ".active")
	if err != nil {
		return err
	}
	if _, err = l.dir.Install(name, func(w io.Writer) error { return WriteTailHeader(w, c.Start) }); err != nil {
		return l.fail(err)
	}
	f, err := os.OpenFile(filepath.Join(l.path, name), os.O_RDWR, 0) // #nosec G304 G703 -- Internally generated tail name in the owned log directory.
	if err != nil {
		return l.fail(err)
	}
	defer func() {
		if f != nil {
			err = errors.Join(err, f.Close())
		}
	}()
	tail, resident, err := recoverResidentTail(f, nil, l.cache != nil)
	if err != nil {
		return l.fail(err)
	}
	c.Active = &TailRef{File: name, Start: c.Start}
	c.Revision++
	if err := l.catalog.publishReset(c); err != nil {
		return l.fail(err)
	}
	old := l.file
	l.file, l.tail, l.resident, l.framed = f, tail, resident, 0
	f = nil
	l.cursorEpoch = new(byte)
	if l.cache != nil {
		l.cache = newSegmentCache(l.cache.recentLimit, l.cache.historicalLimit)
	}
	if err := old.Close(); err != nil {
		return l.fail(err)
	}
	return nil
}
