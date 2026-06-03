package wal_test

import (
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
	"github.com/philborlin/committed/internal/cluster/db/parser"
	"github.com/philborlin/committed/internal/cluster/db/wal"
)

// onDiskTimeSeriesOptions opens with a real on-disk time-series store (NOT the
// in-memory one testOpenOptions uses) so the discard-and-rebuild recovery path
// is actually exercised. fsync stays off purely for speed — a clean Close
// still flushes everything, which is all these tests rely on.
var onDiskTimeSeriesOptions = []wal.Option{wal.WithoutFsync()}

// The time-series watermark bbolt location. Duplicated here as literals rather
// than referenced from the (unexported) wal constants so the test can roll the
// watermark back from the outside, simulating a crash whose final flush never
// recorded it. Keep in sync with tsAppliedIndexBucket/Key in wal_storage.go.
var (
	testTSWatermarkBucket = []byte("tsAppliedIndex")
	testTSWatermarkKey    = []byte("idx")
)

// TestTimeSeriesRecovery_CleanRestartKeepsView verifies the cheap path: after
// a clean Close the watermark equals appliedIndex, so Open trusts the on-disk
// view verbatim and rebuilds nothing. The proof that nothing was rebuilt is a
// point OLDER than the retention window: a rebuild is bounded to that window
// and would drop it, but a trusted on-disk view keeps it.
func TestTimeSeriesRecovery_CleanRestartKeepsView(t *testing.T) {
	dir := t.TempDir()
	p := parser.New()
	const metric = "events"

	s, err := wal.Open(dir, p, nil, nil, onDiskTimeSeriesOptions...)
	require.Nil(t, err)

	now := time.Now()
	oldStamp := now.Add(-400 * time.Hour) // beyond the 336h retention window
	recent := now.Add(-time.Hour)

	// index 1: the type. index 2: a point older than retention. indices 3..5:
	// three recent points.
	insertTypes(t, s, []*cluster.Type{{ID: metric, Name: metric}}, 1, 1)
	saveEntity(t, userEvent(metric, "old", oldStamp), s, 1, 2)
	const recentCount = 3
	for i := 0; i < recentCount; i++ {
		saveEntity(t, userEvent(metric, string(rune('a'+i)), recent.Add(time.Duration(i)*time.Second)), s, 1, uint64(3+i))
	}
	require.Equal(t, uint64(2+recentCount), s.AppliedIndex())
	require.Nil(t, s.Close())

	s2, err := wal.Open(dir, p, nil, nil, onDiskTimeSeriesOptions...)
	require.Nil(t, err)
	defer s2.Close()

	require.Equal(t, uint64(1), totalTimePoints(t, s2, metric, oldStamp.Add(-time.Minute), oldStamp.Add(time.Minute)),
		"a clean restart must keep the pre-retention-window point (it trusts the on-disk view, it does not rebuild)")
	require.Equal(t, uint64(recentCount), totalTimePoints(t, s2, metric, recent.Add(-time.Minute), now),
		"the recent points must still be present and not duplicated")
}

// TestTimeSeriesRecovery_RebuildAfterUncleanShutdown verifies the recovery
// path. We roll the watermark back below appliedIndex to mimic a crash, so Open
// can't trust the on-disk view: it discards it and rebuilds from the event log
// over the retention window. Two things must hold: the recent points come back
// EXACTLY once (the discard prevents double-counting the flushed on-disk copy
// that a naive gap-replay would re-add), and the pre-window point is gone
// (the rebuild is bounded to the retention window).
func TestTimeSeriesRecovery_RebuildAfterUncleanShutdown(t *testing.T) {
	dir := t.TempDir()
	p := parser.New()
	const metric = "events"

	s, err := wal.Open(dir, p, nil, nil, onDiskTimeSeriesOptions...)
	require.Nil(t, err)

	now := time.Now()
	oldStamp := now.Add(-400 * time.Hour)
	recent := now.Add(-time.Hour)

	insertTypes(t, s, []*cluster.Type{{ID: metric, Name: metric}}, 1, 1)
	saveEntity(t, userEvent(metric, "old", oldStamp), s, 1, 2)
	const recentCount = 3
	for i := 0; i < recentCount; i++ {
		saveEntity(t, userEvent(metric, string(rune('a'+i)), recent.Add(time.Duration(i)*time.Second)), s, 1, uint64(3+i))
	}
	require.Equal(t, uint64(2+recentCount), s.AppliedIndex())
	require.Nil(t, s.Close()) // clean: flushes all points to disk, watermark=5

	// Simulate the crash: the on-disk points survive (they were flushed) but
	// the watermark never advanced to cover them.
	rollBackTSWatermark(t, dir, 0)

	s2, err := wal.Open(dir, p, nil, nil, onDiskTimeSeriesOptions...)
	require.Nil(t, err)
	defer s2.Close()

	require.Equal(t, uint64(recentCount), totalTimePoints(t, s2, metric, recent.Add(-time.Minute), now),
		"rebuild must reproduce the recent points exactly once — discarding the on-disk copy is what prevents a double count")
	require.Equal(t, uint64(0), totalTimePoints(t, s2, metric, oldStamp.Add(-time.Minute), oldStamp.Add(time.Minute)),
		"rebuild is bounded to the retention window, so the pre-window point must NOT come back")
}

// TestTimeSeriesRecovery_LagCursorAfterRebuild checks that a rebuild repopulates
// the lag-gauge cursor (TimeSeriesLatestUnixNano) from the newest replayed
// point, so the committed_tstorage_lag gauge is meaningful immediately after an
// unclean restart rather than reading zero until the next apply.
func TestTimeSeriesRecovery_LagCursorAfterRebuild(t *testing.T) {
	dir := t.TempDir()
	p := parser.New()
	const metric = "events"

	s, err := wal.Open(dir, p, nil, nil, onDiskTimeSeriesOptions...)
	require.Nil(t, err)

	recent := time.Now().Add(-time.Hour)
	insertTypes(t, s, []*cluster.Type{{ID: metric, Name: metric}}, 1, 1)
	newest := recent.Add(2 * time.Second)
	saveEntity(t, userEvent(metric, "a", recent), s, 1, 2)
	saveEntity(t, userEvent(metric, "b", newest), s, 1, 3)
	require.Nil(t, s.Close())

	rollBackTSWatermark(t, dir, 0)

	s2, err := wal.Open(dir, p, nil, nil, onDiskTimeSeriesOptions...)
	require.Nil(t, err)
	defer s2.Close()

	require.Equal(t, newest.UnixMilli(), s2.TimeSeriesLatestUnixNano(),
		"after a rebuild the lag cursor must track the newest replayed point")
}

func userEvent(metric, key string, stamp time.Time) *cluster.Entity {
	return &cluster.Entity{
		Type:      &cluster.Type{ID: metric},
		Key:       []byte(key),
		Data:      []byte("v"),
		Timestamp: stamp.UnixMilli(),
	}
}

// totalTimePoints sums the bucketed counts TimePoints reports for metric over
// [start, end). With one point per event, the sum is the number of events in
// the window.
func totalTimePoints(t *testing.T, s db.Storage, metric string, start, end time.Time) uint64 {
	t.Helper()
	points, err := s.TimePoints(metric, start, end)
	require.Nil(t, err)
	var total uint64
	for _, p := range points {
		total += p.Value
	}
	return total
}

// rollBackTSWatermark reopens the node's bbolt store directly and rewrites the
// time-series watermark to idx, simulating a crash whose last flush never
// recorded the true appliedIndex. The wal handle must be closed first.
func rollBackTSWatermark(t *testing.T, dir string, idx uint64) {
	t.Helper()
	dbFile := filepath.Join(dir, "metadata", "bbolt.db")
	kv, err := bolt.Open(dbFile, 0o600, &bolt.Options{Timeout: time.Second})
	require.Nil(t, err)
	defer kv.Close()
	require.Nil(t, kv.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(testTSWatermarkBucket)
		require.NotNil(t, b, "tsAppliedIndex bucket should exist after a clean Close")
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], idx)
		return b.Put(testTSWatermarkKey, buf[:])
	}))
}
