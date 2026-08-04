package db_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// closeRecordingSyncable is a Syncable whose only interesting behavior is
// recording Close — held configs must release what the parse built.
type closeRecordingSyncable struct {
	closed atomic.Bool
}

func (s *closeRecordingSyncable) Sync(context.Context, *cluster.Actual) (cluster.ShouldSnapshot, error) {
	return false, nil
}
func (s *closeRecordingSyncable) Close() error { s.closed.Store(true); return nil }

// closeRecordingIngestable is the ingest twin.
type closeRecordingIngestable struct {
	closed atomic.Bool
}

func (i *closeRecordingIngestable) Ingest(ctx context.Context, _ cluster.Position, _ chan<- *cluster.Proposal, _ chan<- cluster.Position) error {
	<-ctx.Done()
	return ctx.Err()
}

func (i *closeRecordingIngestable) Status(context.Context, cluster.Position) (cluster.IngestableStatus, error) {
	return cluster.IngestableStatus{}, nil
}
func (i *closeRecordingIngestable) Close() error { i.closed.Store(true); return nil }

func newSafeModeDB(t *testing.T, safe bool) (*db.DB, *observer.ObservedLogs) {
	t.Helper()
	dir := t.TempDir()
	p := parser.New()
	walOpts := []wal.Option{wal.WithoutFsync()}
	if safe {
		walOpts = append(walOpts, wal.WithSafeMode())
	}
	s, err := wal.Open(dir, p, nil, nil, walOpts...)
	require.NoError(t, err)

	core, logs := observer.New(zapcore.DebugLevel)
	id := uint64(1)
	dbOpts := []db.Option{db.WithTickInterval(testTickInterval), db.WithLogger(zap.New(core))}
	if safe {
		dbOpts = append(dbOpts, db.WithSafeMode())
	}
	d := db.New(id, db.Peers{id: ""}, s, p, nil, nil, dbOpts...)
	t.Cleanup(func() {
		_ = d.Close()
		_ = s.Close()
	})
	return d, logs
}

// warnContaining reports whether any Warn-level entry's message contains sub.
func warnContaining(logs *observer.ObservedLogs, sub string) bool {
	for _, e := range logs.FilterLevelExact(zapcore.WarnLevel).All() {
		if strings.Contains(e.Message, sub) {
			return true
		}
	}
	return false
}

// TestSafeMode_HoldsWorkersAndReleasesResources pins the escape hatch's core
// contract: with WithSafeMode, Sync and Ingest return nil WITHOUT installing a
// worker — so a config whose worker would deterministically crash the node
// cannot crashloop it — and the built syncable/ingestable is Closed (a held
// config must not leak destination/source resources on every apply or
// reconcile). Deletes still work against the empty registry: DELETE over the
// API is exactly what the operator boots safe mode to do.
func TestSafeMode_HoldsWorkersAndReleasesResources(t *testing.T) {
	d, logs := newSafeModeDB(t, true)
	ctx := context.Background()

	s := &closeRecordingSyncable{}
	require.NoError(t, d.Sync(ctx, "held-sync", s))
	require.False(t, d.HasSyncWorkerForTest("held-sync"),
		"safe mode must not install a sync worker")
	require.True(t, s.closed.Load(),
		"a held syncable must be Closed, not leaked")

	i := &closeRecordingIngestable{}
	require.NoError(t, d.Ingest(ctx, "held-ingest", i))
	require.False(t, d.HasIngestWorkerForTest("held-ingest"),
		"safe mode must not install an ingest worker")
	require.True(t, i.closed.Load(),
		"a held ingestable must be Closed, not leaked")

	// The operator's whole reason to be here: delete works with no worker —
	// and each skipped external teardown is announced, not silent (a deleted
	// ingestable's replication slot pins the source's WAL until dropped).
	require.NotPanics(t, func() { d.DeleteSyncForTest("held-sync") })
	require.True(t, warnContaining(logs, "destination teardown skipped"),
		"safe-mode syncable delete must announce the skipped teardown")
	require.NotPanics(t, func() { d.DeleteIngestForTest("held-ingest") })
	require.True(t, warnContaining(logs, "source-side teardown skipped"),
		"safe-mode ingestable delete must announce the skipped teardown")
}

// Without the flag the same calls install workers — the control that pins the
// gate to the OPTION, not to some environmental accident.
func TestSafeMode_OffSpawnsWorkers(t *testing.T) {
	d, _ := newSafeModeDB(t, false)
	ctx := context.Background()

	s := &closeRecordingSyncable{}
	require.NoError(t, d.Sync(ctx, "live-sync", s))
	require.True(t, d.HasSyncWorkerForTest("live-sync"),
		"normal mode must install the sync worker")
	require.False(t, s.closed.Load(),
		"a live syncable must not be Closed at spawn")
}
