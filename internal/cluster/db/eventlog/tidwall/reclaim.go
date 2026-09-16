package tidwall

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/durablefs"
)

func managedFile(name string) bool {
	if len(name) < 20 {
		return false
	}
	for _, c := range name[:20] {
		if c < '0' || c > '9' {
			return false
		}
	}
	switch name[20:] {
	case "", ".zst", ".zst.SEAL", ".START", ".END":
		return true
	}
	return false
}

// Reclaim verifies CURRENT before deleting regular managed files in obsolete
// generations. Unknown files and symlinks are retained. Every removal is synced.
func (l *Log) Reclaim(ctx context.Context) (result eventlog.ReclaimResult, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e := l.usable(); e != nil {
		return result, e
	}
	if ctx == nil {
		return result, eventlog.ErrInvalid
	}
	if e := ctx.Err(); e != nil {
		return result, e
	}
	current, e := readManifest(l.path)
	if e != nil {
		return result, l.fail(e)
	}
	if current != l.state {
		return result, l.fail(eventlog.ErrCorrupt)
	}
	last, has, e := l.verify()
	if e != nil {
		return result, l.fail(e)
	}
	if last != l.last || has != l.has {
		return result, l.fail(eventlog.ErrCorrupt)
	}
	if e = l.syncData(l.data, l.state.Directory); e != nil {
		return result, l.fail(e)
	}
	entries, e := os.ReadDir(l.path)
	if e != nil {
		return result, l.fail(e)
	}
	for _, entry := range entries {
		if e = ctx.Err(); e != nil {
			return result, e
		}
		name := entry.Name()
		if name == "CURRENT" || name == l.state.Directory {
			continue
		}
		if strings.HasPrefix(name, ".segmentlog-") && entry.Type().IsRegular() {
			info, e := entry.Info()
			if e != nil {
				return result, l.fail(e)
			}
			removed, e := l.dir.Remove(name)
			if removed {
				result.RemovedFiles++
				result.RemovedBytes += uint64(info.Size())
			}
			if e != nil {
				return result, l.fail(e)
			}
			continue
		}
		if !validDirectory(name) || !entry.IsDir() {
			result.SkippedEntries++
			continue
		}
		path := filepath.Join(l.path, name)
		dir, e := durablefs.Open(path)
		if e != nil {
			return result, l.fail(e)
		}
		files, e := os.ReadDir(path)
		if e != nil {
			return result, l.fail(e)
		}
		for _, f := range files {
			if e = ctx.Err(); e != nil {
				return result, e
			}
			if !f.Type().IsRegular() || !managedFile(f.Name()) {
				result.SkippedEntries++
				continue
			}
			info, e := f.Info()
			if e != nil {
				return result, l.fail(e)
			}
			removed, e := dir.Remove(f.Name())
			if removed {
				result.RemovedFiles++
				result.RemovedBytes += uint64(info.Size())
			}
			if e != nil {
				return result, l.fail(e)
			}
		}
		remaining, e := os.ReadDir(path)
		if e != nil {
			return result, l.fail(e)
		}
		if len(remaining) == 0 {
			if e = os.Remove(path); e != nil && !errors.Is(e, os.ErrNotExist) {
				return result, l.fail(e)
			}
			if e = l.dir.Sync(); e != nil {
				return result, l.fail(e)
			}
		}
	}
	return result, nil
}
