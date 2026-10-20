package wal

import (
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
	// Quiesce every Open-started worker that touches the event-log handle
	// (scrub worker AND sealer): these tests poke the handle directly,
	// outside eventMu — safe only with no concurrent reader. Both stops are
	// idempotent, so callers that also stop explicitly are unaffected.
	s.stopScrubWorker()
	s.stopSealer()
	s.stopSealer()
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
	s.stopSealer()

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
