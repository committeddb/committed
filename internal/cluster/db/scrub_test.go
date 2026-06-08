package db_test

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// newWalDBScrub is newWalDB with an explicit automatic-scrub interval (0
// disables the scheduler, leaving only the manual db.Scrub lever).
func newWalDBScrub(t *testing.T, scrubInterval time.Duration) (*db.DB, *wal.Storage) {
	t.Helper()
	dir := t.TempDir()
	p := parser.New()
	s, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)

	id := uint64(1)
	peers := db.Peers{id: ""}
	d := db.New(id, peers, s, p, nil, nil,
		db.WithTickInterval(1*time.Millisecond),
		db.WithScrubInterval(scrubInterval),
	)
	t.Cleanup(func() { _ = d.Close() })
	return d, s
}

func proposeUserUpsert(t *testing.T, d *db.DB, typeID, key, data string) {
	t.Helper()
	ty, err := d.ResolveType(cluster.LatestTypeRef(typeID))
	require.NoError(t, err)
	e := cluster.NewUpsertEntity(ty, []byte(key), []byte(data))
	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{e}}))
}

func proposeUserDelete(t *testing.T, d *db.DB, typeID, key string) {
	t.Helper()
	ty, err := d.ResolveType(cluster.LatestTypeRef(typeID))
	require.NoError(t, err)
	e := cluster.NewDeleteEntity(ty, []byte(key))
	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{e}}))
}

// keyState scans the permanent event log via a fresh Reader and reports whether
// an upsert for key (PII) and/or a delete-tombstone for key are present.
func keyState(t *testing.T, s *wal.Storage, key string) (upsert, tombstone bool) {
	t.Helper()
	r := s.Reader("scrub-verify")
	for {
		a, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		for _, e := range a.Entities {
			if string(e.Key) != key {
				continue
			}
			if e.IsDelete() {
				tombstone = true
			} else {
				upsert = true
			}
		}
	}
	return upsert, tombstone
}

// TestScrubManualRemovesPII drives the manual lever (db.Scrub): after a user
// delete, an explicit Scrub physically removes the subject's PII from the
// permanent event log while keeping the delete-tombstone and unrelated data.
func TestScrubManualRemovesPII(t *testing.T) {
	d, s := newWalDBScrub(t, 0) // scheduler off; exercise only the manual lever

	require.NoError(t, d.ProposeType(testCtx(t), createType("events").config))
	proposeUserUpsert(t, d, "events", "alice", `{"pii":true}`)
	proposeUserUpsert(t, d, "events", "bob", `{"ok":1}`)
	proposeUserDelete(t, d, "events", "alice")

	// Precondition: alice's PII is present before the scrub.
	up, _ := keyState(t, s, "alice")
	require.True(t, up, "alice PII should exist before scrub")

	require.NoError(t, d.Scrub(testCtx(t)))

	require.Eventually(t, func() bool {
		up, tomb := keyState(t, s, "alice")
		return !up && tomb // PII gone, tombstone retained
	}, 5*time.Second, 10*time.Millisecond, "manual scrub did not remove alice's PII")

	// Unrelated data is untouched.
	bobUp, _ := keyState(t, s, "bob")
	require.True(t, bobUp, "bob's data must survive the scrub")
}

// TestScrubAutomaticRemovesPII verifies the automatic scheduler: with a short
// interval and no manual call, the leader proposes a Scrub once a delete creates
// RTBF backlog, and the worker removes the PII.
func TestScrubAutomaticRemovesPII(t *testing.T) {
	d, s := newWalDBScrub(t, 20*time.Millisecond)

	require.NoError(t, d.ProposeType(testCtx(t), createType("events").config))
	proposeUserUpsert(t, d, "events", "alice", `{"pii":true}`)
	proposeUserDelete(t, d, "events", "alice")

	require.Eventually(t, func() bool {
		up, tomb := keyState(t, s, "alice")
		return !up && tomb
	}, 5*time.Second, 10*time.Millisecond, "automatic scrub did not remove alice's PII")

	// The backlog clears when the worker finishes advancing its "completed"
	// bound, which can land a beat after the PII rewrite is observable above —
	// poll rather than assert immediately, or this races under -race load.
	require.Eventually(t, func() bool {
		return !s.HasScrubBacklog()
	}, 5*time.Second, 10*time.Millisecond, "backlog should clear once the scrub completes")
}
