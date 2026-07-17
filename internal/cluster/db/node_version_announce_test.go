package db_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
	"github.com/committeddb/committed/internal/version"
)

// waitForMemberVersion polls MemberVersionForTest(id) until it reports want, or
// fails on timeout. The announce is asynchronous (waits for this node to be a
// member with a known leader, then proposes), so callers poll.
func waitForMemberVersion(t *testing.T, d *db.DB, id, want uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got, ok := d.MemberVersionForTest(id); ok && got == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	got, ok := d.MemberVersionForTest(id)
	t.Fatalf("MemberVersion(%d) did not reach %d within timeout (have %d ok=%v)", id, want, got, ok)
}

// TestAnnounceVersion_SelfAnnounces verifies that with WithVersionAnnounce a
// bootstrap node proposes its version.FeatureLevel into the replicated
// memberVersions map — the announce half of the version-skew gate, exercised
// through the real Propose + apply path.
func TestAnnounceVersion_SelfAnnounces(t *testing.T) {
	dir := t.TempDir()
	p := parser.New()
	s, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	d := db.New(1, db.Peers{1: ""}, s, p, nil, nil,
		db.WithTickInterval(testTickInterval),
		db.WithVersionAnnounce())
	t.Cleanup(func() { _ = d.Close() })

	waitForMemberVersion(t, d, 1, version.FeatureLevel)
}

// TestAnnounceVersion_SurvivesRestart verifies the announced level persists
// across a restart (it lives in the bbolt-backed bucket), so a restarted node
// comes back already counted at its level and does not re-announce.
func TestAnnounceVersion_SurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	p := parser.New()

	s, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	d := db.New(1, db.Peers{1: ""}, s, p, nil, nil,
		db.WithTickInterval(testTickInterval),
		db.WithVersionAnnounce())
	waitForMemberVersion(t, d, 1, version.FeatureLevel)
	require.NoError(t, d.Close())
	require.NoError(t, s.Close())

	s2, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })
	d2 := db.New(1, db.Peers{1: ""}, s2, p, nil, nil,
		db.WithTickInterval(testTickInterval),
		db.WithVersionAnnounce())
	t.Cleanup(func() { _ = d2.Close() })

	lvl, ok := d2.MemberVersionForTest(1)
	require.True(t, ok, "announced level must survive restart")
	require.Equal(t, version.FeatureLevel, lvl)
}
