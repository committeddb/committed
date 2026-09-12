package wal

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
)

// Each log's freeze holds that log's movers and no other's: the event log's
// holds the sealer and the scrub swap, the raft log's holds compaction —
// and they release when the last freeze on that log lifts.
func TestFreezeLayout_EachLogHoldsItsOwnMovers(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, nil, nil, nil, WithoutFsync(), WithEventSegmentSize(2048), WithSealerIdleInterval(25*time.Millisecond))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	eventsDir := filepath.Join(dir, "events")

	for i := uint64(1); i <= 5; i++ {
		e := &pb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(i), Type: pb.EntryNormal.Enum(), Data: []byte("x")}
		require.NoError(t, s.Save(&pb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(i)}, []*pb.Entry{e}, nil))
	}

	// The event log's freeze: the sealer waits, the scrub swap waits, and
	// raft-log compaction is untouched.
	releaseEvents := s.FreezeEventLayout()
	seedEventLog(t, s, 1, 200)
	require.Never(t, func() bool { return countZst(t, eventsDir) > 0 },
		400*time.Millisecond, 25*time.Millisecond, "the sealer compressed under an event-log freeze")
	waited := make(chan error, 1)
	go func() {
		release, err := s.waitLayoutQuiet()
		if err == nil {
			release()
		}
		waited <- err
	}()
	select {
	case err := <-waited:
		t.Fatalf("the scrub swap's wait returned under an event-log freeze: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	require.NotErrorIs(t, s.Compact(2), cluster.ErrCompactionDeferred, "an event-log freeze must not hold raft-log compaction")

	// A second freeze keeps the log held; releasing one does not release all.
	releaseEvents2 := s.FreezeEventLayout()
	releaseEvents()
	releaseEvents() // idempotent
	_, ok := s.eventLayout.move()
	require.False(t, ok)
	releaseEvents2()
	require.NoError(t, <-waited, "the swap wait returns once the last freeze lifts")
	require.Eventually(t, func() bool { return countZst(t, eventsDir) > 0 },
		20*time.Second, 25*time.Millisecond, "the sealer never resumed after the freeze lifted")

	// The raft log's freeze: compaction defers, and nothing else.
	releaseRaft := s.FreezeRaftLayout()
	require.ErrorIs(t, s.Compact(3), cluster.ErrCompactionDeferred, "compaction must defer under a raft-log freeze")
	_, ok = s.eventLayout.move()
	require.True(t, ok, "a raft-log freeze must not hold the event log's movers")
	releaseRaft()
	require.NotErrorIs(t, s.Compact(3), cluster.ErrCompactionDeferred, "compaction runs again after release")
}

// A freeze is a lock held across a mover's step, not a flag the step
// checks first: a freeze taken while a step is in flight waits for the step
// to finish (so what it then lists stays on disk), a step never starts
// under a freeze, and movers never exclude each other.
func TestFreezeLayout_WaitsOutAMoverStep(t *testing.T) {
	s, err := Open(t.TempDir(), nil, nil, nil, WithoutFsync(), WithSealerIdleInterval(time.Hour))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	releaseStep, ok := s.eventLayout.move()
	require.True(t, ok, "no freeze stands, the step may begin")
	releaseOther, ok := s.eventLayout.move()
	require.True(t, ok, "a second mover's step runs beside the first")
	releaseOther()
	frozen := make(chan func(), 1)
	go func() { frozen <- s.FreezeEventLayout() }()
	select {
	case <-frozen:
		t.Fatal("a freeze must wait for the in-flight mover step")
	case <-time.After(150 * time.Millisecond):
	}
	releaseStep()
	var release func()
	select {
	case release = <-frozen:
	case <-time.After(5 * time.Second):
		t.Fatal("the freeze never acquired the layout after the step released it")
	}

	_, ok = s.eventLayout.move()
	require.False(t, ok, "a step must not begin under a freeze")
	release()
	releaseStep, ok = s.eventLayout.move()
	require.True(t, ok, "steps resume once the freeze lifts")
	releaseStep()
}
