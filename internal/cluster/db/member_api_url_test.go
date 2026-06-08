package db_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// waitForMemberAPIURL polls c.MemberAPIURL(id) until it reports want, or fails
// the test on timeout. The announce is asynchronous (it waits for this node to
// become a member with a known leader, then proposes), so callers poll.
func waitForMemberAPIURL(t *testing.T, c interface {
	MemberAPIURL(uint64) (string, bool)
}, id uint64, want string,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got, ok := c.MemberAPIURL(id); ok && got == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	got, ok := c.MemberAPIURL(id)
	t.Fatalf("MemberAPIURL(%d) did not reach %q within timeout (have %q ok=%v)", id, want, got, ok)
}

// TestAnnounceAPIURL_BootstrapNode verifies that a bootstrap node (one that
// StartNode'd the cluster, never going through `member add`) self-announces its
// advertised API URL into the replicated map. This is the case the conf-change
// Context approach could not cover, and the reason we self-announce.
func TestAnnounceAPIURL_BootstrapNode(t *testing.T) {
	dir := t.TempDir()
	p := parser.New()
	s, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)

	d := db.New(1, db.Peers{1: ""}, s, p, nil, nil,
		db.WithTickInterval(testTickInterval),
		db.WithAdvertisedAPIURL("http://n1:8080"))
	t.Cleanup(func() { _ = d.Close() })

	waitForMemberAPIURL(t, d, 1, "http://n1:8080")

	// Membership reports the announced URL and the node as a voter; on the
	// (single-node) leader it carries a match index.
	m := d.Membership()
	require.Equal(t, uint64(1), m.NodeID)
	require.True(t, m.IsLeader)
	require.Len(t, m.Members, 1)
	require.Equal(t, uint64(1), m.Members[0].ID)
	require.Equal(t, cluster.MemberRoleVoter, m.Members[0].Role)
	require.Equal(t, "http://n1:8080", m.Members[0].APIURL)
	require.NotNil(t, m.Members[0].MatchIndex)
}

// TestAnnounceAPIURL_SurvivesRestart verifies the announced URL persists across
// a restart (it lives in the bbolt-backed bucket), so a restarted node comes
// back with its address already known to the cluster.
func TestAnnounceAPIURL_SurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	p := parser.New()

	s, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	d := db.New(1, db.Peers{1: ""}, s, p, nil, nil,
		db.WithTickInterval(testTickInterval),
		db.WithAdvertisedAPIURL("http://n1:8080"))
	waitForMemberAPIURL(t, d, 1, "http://n1:8080")
	// db.Close tears down raft but does not own the storage; close it so the
	// bbolt file lock is released before we reopen the same data dir.
	require.NoError(t, d.Close())
	require.NoError(t, s.Close())

	// Reopen on the same data dir. The URL is read back from storage before
	// any re-announce could run.
	s2, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	require.Equal(t, "http://n1:8080", func() string { u, _ := s2.MemberAPIURL(1); return u }())

	d2 := db.New(1, db.Peers{1: ""}, s2, p, nil, nil,
		db.WithTickInterval(testTickInterval),
		db.WithAdvertisedAPIURL("http://n1:8080"))
	t.Cleanup(func() { _ = d2.Close(); _ = s2.Close() })

	got, ok := d2.MemberAPIURL(1)
	require.True(t, ok)
	require.Equal(t, "http://n1:8080", got)
}

// TestAnnounceAPIURL_DisabledWhenUnset verifies that with no advertised URL the
// node announces nothing — the documented degraded path. We give the node time
// to elect itself and (not) announce, then assert its address stays unknown.
func TestAnnounceAPIURL_DisabledWhenUnset(t *testing.T) {
	dir := t.TempDir()
	p := parser.New()
	s, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)

	d := db.New(1, db.Peers{1: ""}, s, p, nil, nil,
		db.WithTickInterval(testTickInterval))
	t.Cleanup(func() { _ = d.Close() })

	// Let leadership settle so the announce goroutine would have fired if it
	// were going to.
	require.Eventually(t, func() bool { return d.Leader() == 1 }, 5*time.Second, 5*time.Millisecond)
	time.Sleep(50 * time.Millisecond)

	_, ok := d.MemberAPIURL(1)
	require.False(t, ok, "no URL should be announced when WithAdvertisedAPIURL is unset")
}
