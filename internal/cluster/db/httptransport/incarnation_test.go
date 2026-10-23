package httptransport

import (
	"bytes"
	"fmt"
	httpgo "net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
)

// incarnationRaft is a recordingRaft with an incarnation: it refuses senders
// whose incarnation differs from the one it holds for them, and records when
// it is refused as a duplicate.
type incarnationRaft struct {
	*recordingRaft
	self cluster.Incarnation

	mu        sync.Mutex
	known     map[uint64]cluster.Incarnation
	admitted  []cluster.Incarnation
	refusedBy chan uint64
}

func newIncarnationRaft(t *testing.T) *incarnationRaft {
	t.Helper()
	return &incarnationRaft{recordingRaft: newRecordingRaft(), self: mustIncarnation(t), known: map[uint64]cluster.Incarnation{}, refusedBy: make(chan uint64, 4)}
}

func (r *incarnationRaft) Incarnation() (cluster.Incarnation, bool) { return r.self, true }

func (r *incarnationRaft) AdmitIncarnation(from uint64, inc cluster.Incarnation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.admitted = append(r.admitted, inc)
	if k, ok := r.known[from]; ok && k != inc {
		return fmt.Errorf("%w: node %d is %s", db.ErrDuplicateNode, from, k)
	}
	return nil
}

func (r *incarnationRaft) RefusedAsDuplicate(peer uint64, _ string) { r.refusedBy <- peer }

func mustIncarnation(t *testing.T) cluster.Incarnation {
	t.Helper()
	inc, err := cluster.NewIncarnation()
	require.NoError(t, err)
	return inc
}

// A raft message carries its sender's incarnation; a receiver that holds
// another incarnation for the sender's node id refuses it (never stepping it),
// and the sender, told it is a duplicate, stops.
func TestIncarnation_DuplicateSenderIsRefusedAndStops(t *testing.T) {
	receiver := newIncarnationRaft(t)
	trB := New(2, nil, zap.NewNop(), receiver, nil, nil, "")
	srvB := httptest.NewServer(trB.handler())
	defer srvB.Close()

	original := newIncarnationRaft(t)
	trA := New(1, nil, zap.NewNop(), original, nil, nil, "")
	defer trA.Stop()
	require.NoError(t, trA.AddPeer(raft.Peer{ID: 2, Context: []byte(srvB.URL)}))
	trA.Send([]*raftpb.Message{{Type: raftpb.MsgHeartbeat.Enum(), From: proto.Uint64(1), To: proto.Uint64(2)}})
	require.Eventually(t, func() bool { return receiver.processedCount() == 1 }, 5*time.Second, 10*time.Millisecond)
	receiver.mu.Lock()
	require.Equal(t, []cluster.Incarnation{original.self}, receiver.admitted, "the message carried its sender's incarnation")
	receiver.known[1] = original.self
	receiver.mu.Unlock()

	duplicate := newIncarnationRaft(t)
	trDup := New(1, nil, zap.NewNop(), duplicate, nil, nil, "")
	defer trDup.Stop()
	require.NoError(t, trDup.AddPeer(raft.Peer{ID: 2, Context: []byte(srvB.URL)}))
	trDup.Send([]*raftpb.Message{{Type: raftpb.MsgVote.Enum(), From: proto.Uint64(1), To: proto.Uint64(2)}})
	select {
	case peer := <-duplicate.refusedBy:
		require.Equal(t, uint64(2), peer)
	case <-time.After(5 * time.Second):
		t.Fatal("the duplicate was never told it was refused")
	}
	require.Equal(t, 1, receiver.processedCount(), "the duplicate's vote is never stepped")
	require.Empty(t, original.refusedBy)
}

// A sender with no incarnation (an older release) is admitted unchecked, and
// a malformed one is refused.
func TestIncarnation_OlderSenderIsAdmittedMalformedIsRefused(t *testing.T) {
	receiver := newIncarnationRaft(t)
	srv := serve(t, receiver, "")
	require.Equal(t, httpgo.StatusNoContent, postMessageWithIncarnation(t, srv, ""))
	require.Equal(t, httpgo.StatusBadRequest, postMessageWithIncarnation(t, srv, "not-hex"))
	require.Equal(t, 1, receiver.processedCount())
	receiver.mu.Lock()
	require.Empty(t, receiver.admitted)
	receiver.mu.Unlock()
}

// postMessageWithIncarnation POSTs a heartbeat from node 1 to node 2 carrying
// the given incarnation header (none when empty).
func postMessageWithIncarnation(t *testing.T, srv *httptest.Server, inc string) int {
	t.Helper()
	data, err := marshalMessage(&raftpb.Message{Type: raftpb.MsgHeartbeat.Enum(), To: proto.Uint64(2), From: proto.Uint64(1)})
	require.NoError(t, err)
	req, err := httpgo.NewRequest(httpgo.MethodPost, srv.URL+raftMessagePath, bytes.NewReader(data))
	require.NoError(t, err)
	req.Header.Set(protocolHeader, protocolVersion)
	req.Header.Set(clusterIDHeader, clusterID)
	req.Header.Set(targetNodeHeader, "2")
	if inc != "" {
		req.Header.Set(incarnationHeader, inc)
	}
	resp, err := httpgo.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	return resp.StatusCode
}
