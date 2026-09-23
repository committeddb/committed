package tidwall

import (
	"io"
	"os"
	"path/filepath"

	native "github.com/tidwall/wal"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/durablefs"
)

// CaptureBackup retains the production native format. The caller freezes
// compression, replacement and truncation; appends and rollover may continue.
func (l *LegacyLog) CaptureBackup(visit func(string, int64, func(io.Writer) error) error) error {
	if visit == nil {
		return eventlog.ErrInvalid
	}
	return captureNative(l.log, "", visit)
}

func captureNative(log *native.Log, prefix string, visit func(string, int64, func(io.Writer) error) error) error {
	layout, err := log.LayoutSnapshot()
	if err != nil {
		return err
	}
	for _, ref := range layout.Sealed {
		info, err := os.Stat(ref.Path)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(filepath.Join(prefix, filepath.Base(ref.Path)))
		if err := durablefs.CaptureFile(ref.Path, name, info.Size(), visit); err != nil {
			return err
		}
	}
	return durablefs.CaptureFile(layout.Tail.Path, filepath.ToSlash(filepath.Join(prefix, filepath.Base(layout.Tail.Path))), layout.TailLen, visit)
}

// CaptureBackup preserves CURRENT and only its selected generation. The dense
// experimental backend serializes capture with all other operations.
func (l *Log) CaptureBackup(visit func(string, int64, func(io.Writer) error) error) error {
	if visit == nil {
		return eventlog.ErrInvalid
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.usable(); err != nil {
		return err
	}
	path := filepath.Join(l.path, "CURRENT")
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if err := durablefs.CaptureFile(path, "CURRENT", info.Size(), visit); err != nil {
		return err
	}
	return captureNative(l.data, l.state.Directory, visit)
}
