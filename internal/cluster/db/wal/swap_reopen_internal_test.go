package wal

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// fatalPanicStorage opens a Storage whose logger PANICS instead of os.Exit-ing on
// Fatal, so a test can assert the reopen-or-fatal helpers fatal at the swap site.
func fatalPanicStorage(t *testing.T) *Storage {
	t.Helper()
	core, _ := observer.New(zap.DebugLevel)
	logger := zap.New(core, zap.WithFatalHook(zapcore.WriteThenPanic))
	s, err := Open(t.TempDir(), nil, nil, nil, WithoutFsync(), WithLogger(logger))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// After a swap closes the live bbolt handle, reopenKVAfterSwapOrFatal reopens it
// so the apply path never observes a closed handle (ErrDatabaseNotOpen).
func TestReopenKVAfterSwap_ReopensClosedHandle(t *testing.T) {
	s, err := Open(t.TempDir(), nil, nil, nil, WithoutFsync())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	// Quiesce the scrub worker Open started: its one-shot startup runPendingScrub
	// reads keyValueStorage via s.view, which would race this test's bare (no-kvMu)
	// handle swap below. stopScrubWorker waits for that startup pass to finish and
	// is idempotent, so t.Cleanup's Close stays well-defined.
	s.stopScrubWorker()

	boltPath := s.keyValueStorage.Path()
	require.NoError(t, s.keyValueStorage.Close()) // the post-close point in the swap

	s.reopenKVAfterSwapOrFatal(boltPath, &bolt.Options{Timeout: time.Second}, "test")

	require.NoError(t, s.view(func(*bolt.Tx) error { return nil }),
		"apply path must see a live handle after the swap, not ErrDatabaseNotOpen")
}

// When the reopen itself fails, the helper fatals AT THE SWAP SITE (here: panics
// via the fatal hook) rather than returning a closed handle into the
// survive-and-continue scrub worker, where a later apply would crash
// mis-attributed to the apply path.
func TestReopenKVAfterSwap_FatalsWhenReopenFails(t *testing.T) {
	s := fatalPanicStorage(t)
	// A path whose parent directory does not exist cannot be opened as bbolt.
	badPath := filepath.Join(t.TempDir(), "missing", "bbolt.db")
	require.Panics(t, func() {
		s.reopenKVAfterSwapOrFatal(badPath, &bolt.Options{Timeout: time.Second}, "test")
	})
}

// The event-log analogue: after a swap closes the live event log,
// reopenEventLogAfterSwapOrFatal reopens it so appendEvent never sees ErrClosed.
func TestReopenEventLogAfterSwap_ReopensClosedHandle(t *testing.T) {
	s, err := Open(t.TempDir(), nil, nil, nil, WithoutFsync())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	// Quiesce the Open-started scrub worker before the bare handle swap, as in
	// TestReopenKVAfterSwap_ReopensClosedHandle.
	s.stopScrubWorker()

	require.NoError(t, s.eventLog.Close()) // the post-close point in the swap

	s.reopenEventLogAfterSwapOrFatal("test")

	_, err = s.eventLog.LastIndex()
	require.NoError(t, err, "event log must be live after the swap, not ErrClosed")
}

// When reopening the event log fails, the helper fatals at the swap site rather
// than reopening a missing dir (which would silently create an empty log).
func TestReopenEventLogAfterSwap_FatalsWhenReopenFails(t *testing.T) {
	s := fatalPanicStorage(t)
	// Point eventLogDir at a path whose parent is a regular file, so wal.Open's
	// MkdirAll fails (ENOTDIR) instead of silently creating an empty log.
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0o600))
	orig := s.eventLogDir
	s.eventLogDir = filepath.Join(blocker, "events")
	require.Panics(t, func() { s.reopenEventLogAfterSwapOrFatal("test") })
	s.eventLogDir = orig // restore so t.Cleanup's Close is well-defined
}
