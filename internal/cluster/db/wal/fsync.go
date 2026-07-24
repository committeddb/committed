package wal

import (
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster/fsutil"
)

// syncFile fsyncs the file at path to stable storage. It exists to make a file
// written with os.WriteFile durable BEFORE it is renamed over live state:
// os.WriteFile does not fsync, so a crash after the rename could otherwise
// surface a torn or zero-length file that the next bolt.Open cannot read. Called
// on the pre-rename temp file, where a failure means the swap must abort (the
// live file is still untouched), so the error is returned rather than tolerated.
// No-op when fsync is disabled (the WithoutFsync test option), keeping the wal
// test suite off the fsync path. Delegates to fsutil so the storage engine and the
// backup/repair tooling share one fsync implementation.
func (s *Storage) syncFile(path string) error {
	if s.fsyncDisabled {
		return nil
	}
	return fsutil.SyncFile(path)
}

// syncDirBestEffort fsyncs a directory's entry list so a rename into or out of it
// is crash-durable — a rename is atomic for a concurrent reader's visibility but
// is not persisted until the parent directory is fsync'd. It is called AFTER the
// rename has already committed the swap, so a failure is a durability gap (the
// rename is visible and will be re-fsync'd by the next write), not a torn state
// to unwind; it is logged and tolerated rather than returned. No-op when fsync is
// disabled (the WithoutFsync test option).
func (s *Storage) syncDirBestEffort(path, what string) {
	if s.fsyncDisabled {
		return
	}
	if err := fsutil.SyncDir(path); err != nil {
		s.logger.Warn("could not fsync directory after swap; the rename may not survive an immediate crash until the next sync",
			zap.String("op", what), zap.String("dir", path), zap.Error(err))
	}
}
