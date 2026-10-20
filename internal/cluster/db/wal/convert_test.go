package wal_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db/datadir"
	"github.com/committeddb/committed/internal/cluster/db/wal"
	"github.com/committeddb/committed/pkg/segmentlog"
)

func segmentedOptions() []wal.Option {
	return append([]wal.Option{wal.WithSegmentedEventLog(segmentlog.LogOptions{
		Encoding: segmentlog.Options{Compression: segmentlog.ZstdDefault},
	})}, testOpenOptions...)
}

// scrubbedTidwallNode builds a stopped tidwall node whose log has a hole: the
// scrub at 4 erased alice's original at 2. Returns its directory.
func scrubbedTidwallNode(t *testing.T) string {
	t.Helper()
	s := NewStorage(t, nil)
	s.RegisterType(t, "u", 1, 1)
	saveEntity(t, userUpsert("u", "alice", `{"pii":true}`), s, 1, 2)
	saveEntity(t, userUpsert("u", "bob", `{"ok":1}`), s, 1, 3)
	saveEntity(t, userDelete("u", "alice"), s, 1, 4)
	saveEntity(t, userUpsert("u", "carol", `{"ok":2}`), s, 1, 5)
	require.NoError(t, s.RunScrubForTest(4))
	require.NoError(t, s.Close())
	t.Cleanup(func() { _ = os.RemoveAll(s.path) })
	return s.path
}

// requireConvertedNode opens dir on the segmented engine and checks it holds
// scrubbedTidwallNode's log: the same records at the same indexes, the hole
// kept, the scrub generation and append progress carried, no residue left.
func requireConvertedNode(t *testing.T, dir string) {
	t.Helper()
	segmented, err := segmentlog.RecognizeDirectory(datadir.EventsDir(dir))
	require.NoError(t, err)
	require.True(t, segmented)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		require.NotContains(t, e.Name(), "events.", "no scrub temp or retired log is left behind")
	}

	ws, err := wal.Open(dir, nil, nil, nil, segmentedOptions()...)
	require.NoError(t, err)
	s := &StorageWrapper{ws, dir, nil, nil}
	defer func() { _ = ws.Close() }()
	require.Equal(t, uint64(5), s.EventIndex())
	require.Equal(t, s.EventIndex(), s.AppliedIndex())
	require.Equal(t, uint64(4), s.EventLogGeneration())
	_, err = s.ActualAt(2)
	require.ErrorIs(t, err, wal.ErrActualNotFound, "the scrubbed original stays erased")
	for idx, key := range map[uint64]string{3: "bob", 4: "alice", 5: "carol"} {
		a, err := s.ActualAt(idx)
		require.NoError(t, err)
		require.Equal(t, []byte(key), a.Entities[0].Key)
	}
	saveEntity(t, userUpsert("u", "dave", `{"ok":3}`), s, 1, 6)
	a, err := s.ActualAt(6)
	require.NoError(t, err, "the converted log takes new appends")
	require.Equal(t, []byte("dave"), a.Entities[0].Key)
}

func TestConvertEventLog_CarriesTheLogToTheSegmentedEngine(t *testing.T) {
	dir := scrubbedTidwallNode(t)

	_, err := wal.Open(dir, nil, nil, nil, segmentedOptions()...)
	require.ErrorIs(t, err, wal.ErrTidwallEventLog, "the node refuses the log before conversion")

	res, err := wal.ConvertEventLog(dir)
	require.NoError(t, err)
	require.Equal(t, wal.Converted{Records: 4, LastIndex: 5, Generation: 4}, *res)
	requireConvertedNode(t, dir)

	res, err = wal.ConvertEventLog(dir)
	require.NoError(t, err)
	require.True(t, res.AlreadySegmented, "a rerun changes nothing")
}

// An interrupted run leaves either log whole and the command reruns: a half
// built copy is reaped, and a swap that moved events/ aside but never put the
// copy in place rolls back to the original.
func TestConvertEventLog_RerunsAfterAnInterruption(t *testing.T) {
	t.Run("copy", func(t *testing.T) {
		dir := scrubbedTidwallNode(t)
		partial := datadir.ScrubDir(datadir.EventsDir(dir), 4)
		require.NoError(t, os.MkdirAll(partial, 0o750))
		require.NoError(t, os.WriteFile(filepath.Join(partial, "metadata.db"), []byte("torn"), 0o600))
		_, err := wal.ConvertEventLog(dir)
		require.NoError(t, err)
		requireConvertedNode(t, dir)
	})
	t.Run("swap", func(t *testing.T) {
		dir := scrubbedTidwallNode(t)
		events := datadir.EventsDir(dir)
		require.NoError(t, os.Rename(events, datadir.RetiredDir(events)))
		_, err := wal.ConvertEventLog(dir)
		require.NoError(t, err)
		requireConvertedNode(t, dir)
	})
}
