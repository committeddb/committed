package httptransport

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// Drive the actual Raft state machine synchronously, without timers or network
// workers. Transport feedback goes to the same leader that emitted the snapshot.
type snapshotDropRaft struct {
	*recordingRaft
	node *raft.RawNode
}

func (r *snapshotDropRaft) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	r.recordingRaft.ReportSnapshot(id, status)
	r.node.ReportSnapshot(id, status)
}

func (r *snapshotDropRaft) ReportUnreachable(id uint64) {
	r.recordingRaft.ReportUnreachable(id)
	r.node.ReportUnreachable(id)
}

func TestSend_DroppedSnapshotAllowsRetry(t *testing.T) {
	for _, reason := range []string{"missing-peer", "full-queue"} {
		for _, reportFailure := range []bool{false, true} {
			name := reason + "/transport-feedback"
			if reportFailure {
				name = reason + "/explicit-failure-control"
			}
			t.Run(name, func(t *testing.T) {
				storage := raft.NewMemoryStorage()
				require.NoError(t, storage.ApplySnapshot(&raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
					Index: proto.Uint64(10), Term: proto.Uint64(1), ConfState: &raftpb.ConfState{Voters: []uint64{1, 2}},
				}}))
				require.NoError(t, storage.SetHardState(&raftpb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(10)}))
				node, err := raft.NewRawNode(&raft.Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1, Storage: storage, MaxInflightMsgs: 16, MaxSizePerMsg: 4096})
				require.NoError(t, err)
				drain := func() []*raftpb.Message {
					var messages []*raftpb.Message
					for node.HasReady() {
						ready := node.Ready()
						if !raft.IsEmptyHardState(ready.HardState) {
							require.NoError(t, storage.SetHardState(ready.HardState))
						}
						require.NoError(t, storage.Append(ready.Entries))
						messages = append(messages, ready.Messages...)
						node.Advance(ready)
					}
					return messages
				}
				require.NoError(t, node.Campaign())
				drain()
				term := *node.BasicStatus().Term
				require.NoError(t, node.Step(&raftpb.Message{Type: raftpb.MsgVoteResp.Enum(), From: proto.Uint64(2), To: proto.Uint64(1), Term: proto.Uint64(term)}))
				drain()
				require.Equal(t, raft.StateLeader, node.BasicStatus().RaftState)
				// The follower lacks even the compacted prefix and rejects the leader's
				// append probe. Raft must send the stored snapshot to catch it up.
				require.NoError(t, node.Step(&raftpb.Message{Type: raftpb.MsgAppResp.Enum(), From: proto.Uint64(2), To: proto.Uint64(1), Term: proto.Uint64(term), Index: proto.Uint64(10), Reject: proto.Bool(true), RejectHint: proto.Uint64(0)}))
				var snapshot *raftpb.Message
				for _, m := range drain() {
					if m.GetType() == raftpb.MsgSnap {
						snapshot = m
					}
				}
				require.NotNil(t, snapshot, "fixture must emit a real snapshot")
				require.Equal(t, tracker.StateSnapshot, node.Status().Progress[2].State)

				feedback := &snapshotDropRaft{newRecordingRaft(), node}
				transport := New(1, nil, zap.NewNop(), feedback, nil, nil, "")
				defer transport.Stop()
				if reason == "full-queue" {
					// No worker: keep the queue deterministically full during Send.
					p := &peer{id: 2, msgc: make(chan *raftpb.Message, peerQueueDepth), stopc: make(chan struct{})}
					transport.peers[2] = p
					for range peerQueueDepth {
						p.msgc <- &raftpb.Message{Type: raftpb.MsgApp.Enum()}
					}
				}
				transport.Send([]*raftpb.Message{snapshot})
				if reportFailure {
					feedback.ReportSnapshot(2, raft.SnapshotFailure)
				}

				var retries []*raftpb.Message
				for range 5 {
					node.Tick()
					messages := drain()
					heartbeat := false
					for _, m := range messages {
						if m.GetType() == raftpb.MsgHeartbeat && m.GetTo() == 2 {
							heartbeat = true
						}
					}
					require.True(t, heartbeat, "leader must still send heartbeats to the follower")
					require.NoError(t, node.Step(&raftpb.Message{Type: raftpb.MsgHeartbeatResp.Enum(), From: proto.Uint64(2), To: proto.Uint64(1), Term: proto.Uint64(term)}))
					for _, m := range drain() {
						if m.GetType() == raftpb.MsgSnap {
							retries = append(retries, m)
						}
					}
				}
				require.True(t, node.Status().Progress[2].RecentActive, "heartbeat replies must establish follower reachability")
				require.NotEmpty(t, retries, "dropped snapshot was never retried despite successful heartbeats; progress=%+v", node.Status().Progress[2])
				require.Equal(t, uint64(2), retries[0].GetTo())
				require.Equal(t, uint64(10), retries[0].GetSnapshot().GetMetadata().GetIndex())
				status, ok := feedback.snapStatus(2)
				require.True(t, ok, "snapshot drop must be reported to Raft")
				require.Equal(t, raft.SnapshotFailure, status)
			})
		}
	}
}
