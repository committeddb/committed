package httptransport

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// stalledAppendRaft holds every MsgApp inside Process until released, as a
// peer does while it receives and persists a large append, and records the
// other messages as they arrive.
type stalledAppendRaft struct {
	*recordingRaft
	release chan struct{}
	once    sync.Once
	entered chan struct{}
}

func (r *stalledAppendRaft) Process(ctx context.Context, m *raftpb.Message) error {
	if m.GetType() == raftpb.MsgApp {
		r.once.Do(func() { close(r.entered) })
		<-r.release
	}
	return r.recordingRaft.Process(ctx, m)
}

// A heartbeat, a vote and a heartbeat response reach a peer while an append to
// that peer is still in flight: control messages never queue behind appends,
// or a slow append outlasts the election timeout and the cluster churns.
func TestSend_ControlMessagesPassAStalledAppend(t *testing.T) {
	rrB := &stalledAppendRaft{recordingRaft: newRecordingRaft(), release: make(chan struct{}), entered: make(chan struct{})}
	release := sync.OnceFunc(func() { close(rrB.release) })
	defer release()
	trB := New(2, nil, zap.NewNop(), rrB, nil, nil, "")
	srvB := httptest.NewServer(trB.handler())
	defer srvB.Close()
	defer release() // let the stalled POST finish before the server closes

	trA := New(1, nil, zap.NewNop(), newRecordingRaft(), nil, nil, "")
	defer trA.Stop()
	require.NoError(t, trA.AddPeer(raft.Peer{ID: 2, Context: []byte(srvB.URL)}))

	trA.Send([]*raftpb.Message{{Type: raftpb.MsgApp.Enum(), From: proto.Uint64(1), To: proto.Uint64(2)}})
	select {
	case <-rrB.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the append never reached the peer")
	}
	trA.Send([]*raftpb.Message{
		{Type: raftpb.MsgHeartbeat.Enum(), From: proto.Uint64(1), To: proto.Uint64(2)},
		{Type: raftpb.MsgPreVote.Enum(), From: proto.Uint64(1), To: proto.Uint64(2)},
		{Type: raftpb.MsgHeartbeatResp.Enum(), From: proto.Uint64(1), To: proto.Uint64(2)},
	})
	require.Eventually(t, func() bool { return rrB.processedCount() == 3 }, 2*time.Second, 10*time.Millisecond,
		"control messages must reach the peer while an append to it is in flight")

	release()
	require.Eventually(t, func() bool { return rrB.processedCount() == 4 }, 5*time.Second, 10*time.Millisecond,
		"the append is delivered once the peer takes it")
}

// A message's deadline covers its transfer at the slowest link allowed for, so
// a maximum-size append is not cut off on every retry; a small one keeps the
// base deadline.
func TestSendTimeoutGrowsWithTheMessage(t *testing.T) {
	require.Equal(t, requestTimeout, sendTimeout(0))
	require.Greater(t, sendTimeout(64<<20), requestTimeout+6*time.Second)
}
