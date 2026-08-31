package db_test

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// TestRTBFErase_EndToEnd drives the whole delete-key erasure chain the way a
// live node runs it, with no storage-level levers: the node announces feature
// level 4, a consuming syncable checkpoints past the delete, the automatic
// scrub scheduler proposes (first the PII removal, then — once the checkpoint
// evidence lands — the authorized erase), and the retained tombstone's raw
// subject key ends as the erased sentinel. After it, the subject identifier
// has no on-disk copy in the permanent event log at all.
func TestRTBFErase_EndToEnd(t *testing.T) {
	dir := t.TempDir()
	p := parser.New()
	s, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, nil, nil,
		db.WithTickInterval(1*time.Millisecond),
		db.WithScrubInterval(20*time.Millisecond),
		db.WithVersionAnnounce(), // featureEnabled(4) → Scrub commands authorize the erase
	)
	t.Cleanup(func() { _ = d.Close(); _ = s.Close() })

	require.NoError(t, d.ProposeType(testCtx(t), createType("events").config))

	// A registered, consuming syncable: the erase gates on ITS checkpoint.
	const sink = "erase-sink"
	seedSyncableConfig(t, d, sink)
	fake := &clusterfakes.FakeSyncable{}
	fake.SyncReturns(cluster.ShouldSnapshot(true), nil)
	require.NoError(t, d.Sync(testCtx(t), sink, fake))

	proposeUserUpsert(t, d, "events", "alice", `{"pii":true}`)
	proposeUserUpsert(t, d, "events", "bob", `{"ok":1}`)
	proposeUserDelete(t, d, "events", "alice")

	// End state: alice's upsert physically removed AND the tombstone's key
	// erased — the raw identifier is gone from every surviving log record.
	require.Eventually(t, func() bool {
		up, rawTomb, erased := eraseKeyState(t, s, "alice")
		return !up && !rawTomb && erased
	}, 20*time.Second, 20*time.Millisecond,
		"the scheduler-driven erase never completed: the raw subject key is still in the log")

	// The bystander and the erased marker both survive.
	up, _, _ := eraseKeyState(t, s, "bob")
	require.True(t, up, "unrelated data must survive")
}

// eraseKeyState scans the permanent event log and reports whether an upsert
// for key, a RAW-keyed delete-tombstone for key, and an erased-sentinel
// delete-tombstone are present.
func eraseKeyState(t *testing.T, s *wal.Storage, key string) (upsert, rawTombstone, erasedTombstone bool) {
	t.Helper()
	r := s.Reader("erase-verify")
	for {
		a, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		for _, e := range a.Entities {
			switch {
			case e.IsDelete() && string(e.Key) == key:
				rawTombstone = true
			case e.IsDelete() && cluster.IsErasedKey(e.Key):
				erasedTombstone = true
			case string(e.Key) == key:
				upsert = true
			}
		}
	}
	return upsert, rawTombstone, erasedTombstone
}
