package wal

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
)

// A pending scrub does not run over a log being filled from a peer, nor
// over an empty one: it waits for the catch-up's release, which pokes it.
func TestPendingScrub_DefersWhileCatchingUpAndOverAnEmptyLog(t *testing.T) {
	s, err := Open(t.TempDir(), nil, nil, nil, WithoutFsync(), WithSealerIdleInterval(time.Hour), WithLogger(zaptest.NewLogger(t)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	// A bound left pending over an empty log, as a fetched snapshot leaves it;
	// its command, at 11, is not in the log yet.
	require.NoError(t, s.setPendingScrubBound(4, false, 11))
	completed := func() uint64 { _, c := s.ScrubProgress(); return c }

	// Empty log: nothing through the bound is there to rewrite.
	s.signalScrub()
	require.Never(t, func() bool { return completed() == 4 }, 300*time.Millisecond, 20*time.Millisecond,
		"a pending scrub must not run over an empty log")

	// Real proposals, as a fetch would leave them: the rewrite decodes each.
	for i := uint64(1); i <= 10; i++ {
		e, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: fmt.Sprintf("t%d", i), Name: fmt.Sprintf("t%d", i), Version: 1})
		require.NoError(t, err)
		bs, err := (&cluster.Proposal{Entities: []*cluster.Entity{e}}).Marshal()
		require.NoError(t, err)
		ent := &pb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(i), Type: pb.EntryNormal.Enum(), Data: bs}
		require.NoError(t, s.Save(&pb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(i)}, []*pb.Entry{ent}, nil))
		require.NoError(t, s.ApplyCommitted(ent))
	}
	// The scrub command beyond the bound, applied as a node applies it.
	command, err := cluster.NewScrubEntity(4, false)
	require.NoError(t, err)
	bs, err := (&cluster.Proposal{Entities: []*cluster.Entity{command}}).Marshal()
	require.NoError(t, err)
	ent := &pb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(11), Type: pb.EntryNormal.Enum(), Data: bs}
	release := s.BeginCatchUp()
	require.NoError(t, s.Save(&pb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(11)}, []*pb.Entry{ent}, nil))
	require.NoError(t, s.ApplyCommitted(ent))
	s.signalScrub()
	require.Never(t, func() bool { return completed() == 4 }, 300*time.Millisecond, 20*time.Millisecond,
		"a pending scrub must not run while a catch-up fills the log")

	release()
	require.Eventually(t, func() bool { return completed() == 4 }, 10*time.Second, 20*time.Millisecond,
		"the release lets the deferred scrub run, without another signal")
}
