//go:build adversarial

package db_test

import (
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// Drop exactly one recovery snapshot through the real transport's missing-peer
// branch, then restore the peer immediately. Heartbeats remain deliverable.
type learnerSnapshotDropTransport struct {
	db.Transport
	learner raft.Peer
	armed   *atomic.Bool
	dropped chan error
}

func (t *learnerSnapshotDropTransport) Send(messages []*raftpb.Message) {
	for _, m := range messages {
		if m.GetType() == raftpb.MsgSnap && m.GetTo() == t.learner.ID && t.armed.CompareAndSwap(true, false) {
			t.Transport.RemovePeer(t.learner.ID)
			t.Transport.Send([]*raftpb.Message{m})
			t.dropped <- t.Transport.AddPeer(t.learner)
		} else {
			t.Transport.Send([]*raftpb.Message{m})
		}
	}
}

func TestAdversarial_LearnerRestartRetriesDroppedSnapshotWithoutElection(t *testing.T) {
	forLifecycleBackends(t, func(t *testing.T, storageOpts []wal.Option) {
		ports := pickFreePorts(4)
		peers := make([]raft.Peer, 4)
		for i, port := range ports {
			peers[i] = raft.Peer{ID: uint64(i + 1), Context: []byte(fmt.Sprintf("http://127.0.0.1:%d", port))}
		}
		fc := NewFaultyCluster(peers)
		fatalC := make(chan fatalEvent, 16)
		var armed atomic.Bool
		dropped := make(chan error, 1)
		// Allow a one-second election timeout: this test requires a stable leader,
		// and the shared harness's 200ms election timeout can expire under CI load.
		opts := append(catchUpNodeOpts(), db.WithTickInterval(100*time.Millisecond), db.WithTransportWrapperForTest(func(inner db.Transport) db.Transport {
			return &learnerSnapshotDropTransport{Transport: inner, learner: peers[3], armed: &armed, dropped: dropped}
		}))
		// Register directory cleanup before node cleanup (Cleanup runs in reverse).
		root := t.TempDir()
		nodes := make(Rafts, 0, 4)
		t.Cleanup(func() {
			for _, n := range nodes {
				_ = n.Close()
			}
			for _, n := range nodes {
				_ = n.storage.Close()
			}
		})
		for i := 0; i < 3; i++ {
			nodes = append(nodes, openWalRaft(t, peers[i].ID, peers[:3], filepath.Join(root, fmt.Sprint(peers[i].ID)), fc, opts, fatalC, storageOpts...))
		}
		nodes.WaitForLeader(t)
		leader := nodes.LeaderRaft()
		learnerDir := filepath.Join(root, "4")
		joinOpts := append([]db.Option{db.WithJoin()}, opts...)
		learner := openWalRaft(t, 4, peers, learnerDir, fc, joinOpts, fatalC, storageOpts...)
		nodes = append(nodes, learner)
		leader.submitConfChange(addLearnerCC(4, string(peers[3].Context)))
		waitForLearner(t, leader, 4)
		var seq uint64
		proposeAppliedStorageBurst(t, nodes, &seq, 5)
		term := leader.raft.TermForTest()
		pausedAt := learner.storage.AppliedIndex()
		require.NoError(t, learner.Close())
		require.NoError(t, learner.storage.Close())
		proposeAppliedStorageBurst(t, nodes[:3], &seq, 40)
		require.Greater(t, leader.raft.LastCompactedIndexForTest(), pausedAt, "learner must require a snapshot")

		// No messages from the stopped learner can solicit a snapshot. Arm before
		// reopening so its first recovery snapshot exercises the enqueue failure.
		armed.Store(true)
		rebootWalNode(t, learner, learnerDir, joinOpts, fatalC, storageOpts...)
		select {
		case err := <-dropped:
			require.NoError(t, err)
		case <-time.After(15 * time.Second):
			t.Fatal("no recovery snapshot was dropped")
		}
		target := leader.storage.AppliedIndex()
		require.Eventually(t, func() bool { return learner.storage.AppliedIndex() >= target && learner.storage.EventIndex() >= target }, 30*time.Second, 10*time.Millisecond, "learner did not recover after the dropped snapshot")
		require.Positive(t, learner.raft.CatchUpRunsForTest(), "recovery must fetch compacted history")
		proposeAppliedStorageBurst(t, nodes, &seq, 3)
		waitForSurvivorConvergence(t, nodes, 10*time.Second)
		require.Equal(t, leader.id, nodes.LeaderRaft().id, "recovery must not require a different leader")
		require.Equal(t, term, leader.raft.TermForTest(), "recovery must not require any new election")
		requireNoFatal(t, fatalC)
	})
}
