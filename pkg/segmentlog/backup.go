package segmentlog

import (
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/committeddb/committed/internal/durablefs"
)

// CaptureBackup streams a reopenable catalog and its selected files. Names are
// relative to the log directory. visit consumes write synchronously and must not
// reenter maintenance operations. Rewrites, reclamation and Close wait for the
// capture; appends and rollover continue after local catalog spooling completes.
// The captured tail is limited to its acknowledged prefix. No payloads are decoded.
func (l *Log) CaptureBackup(visit func(string, int64, func(io.Writer) error) error) error {
	if visit == nil {
		return ErrInvalid
	}
	l.maintenanceMu.Lock()
	defer l.maintenanceMu.Unlock()
	spool, err := os.CreateTemp(l.path, ".segmentlog-")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(spool.Name()) }()
	var state TailState
	err = func() error {
		l.mu.Lock()
		defer l.mu.Unlock()
		if err := l.usable(); err != nil {
			return err
		}
		var err error
		state, err = l.tail.State()
		if err != nil {
			return err
		}
		return l.catalog.backup(spool)
	}()
	err = errors.Join(err, spool.Close())
	if err != nil {
		return err
	}
	// Read references from the private catalog, not the changing live catalog.
	// This bounds memory and avoids a live read transaction across remote I/O.
	snapshot, err := openBackupCatalog(spool.Name())
	if err != nil {
		return err
	}
	defer func() { _ = snapshot.Close() }()
	head, err := snapshot.head()
	if err != nil {
		return err
	}
	info, err := os.Stat(spool.Name())
	if err != nil {
		return err
	}
	if err = durablefs.CaptureFile(spool.Name(), boltCatalogName, info.Size(), visit); err != nil {
		return err
	}
	for ref, err := range snapshot.ranges(Coverage{Start: head.Start, End: head.Active.Start}) {
		if err != nil {
			return err
		}
		if ref.File == "" {
			continue
		}
		path := filepath.Join(l.path, ref.File)
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		size := info.Size()
		if ref.TailBytes != 0 {
			size = ref.TailBytes
		}
		if err := durablefs.CaptureFile(path, ref.File, size, visit); err != nil {
			return err
		}
	}
	return durablefs.CaptureFile(filepath.Join(l.path, head.Active.File), head.Active.File, state.End, visit)
}
