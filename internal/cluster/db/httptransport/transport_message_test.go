package httptransport

import (
	"bytes"
	"context"
	httpgo "net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// recordingRaft records every callback the transport drives, so a test can
// assert what reached raft (Process), which peers were reported unreachable, and
// how snapshots were reported. IsIDRemoved is controllable for the removed-peer
// guard.
type recordingRaft struct {
	mu          sync.Mutex
	processed   []*raftpb.Message
	unreachable []uint64
	snapshots   map[uint64]raft.SnapshotStatus
	removed     map[uint64]bool
	processErr  error
}

func newRecordingRaft() *recordingRaft {
	return &recordingRaft{snapshots: map[uint64]raft.SnapshotStatus{}, removed: map[uint64]bool{}}
}

func (r *recordingRaft) Process(_ context.Context, m *raftpb.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.processed = append(r.processed, m)
	return r.processErr
}

func (r *recordingRaft) IsIDRemoved(id uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.removed[id]
}

func (r *recordingRaft) ReportUnreachable(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unreachable = append(r.unreachable, id)
}

func (r *recordingRaft) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snapshots[id] = status
}

func (r *recordingRaft) processedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.processed)
}

func (r *recordingRaft) wasUnreachable(id uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Contains(r.unreachable, id)
}

func (r *recordingRaft) snapStatus(id uint64) (raft.SnapshotStatus, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.snapshots[id]
	return s, ok
}

// TestHandler_Guards covers the receive handler's accept/reject decisions — the
// security-relevant ones (wrong cluster, wrong protocol, a removed sender) plus
// method, none of which had direct coverage before.
func TestHandler_Guards(t *testing.T) {
	rr := newRecordingRaft()
	tr := New(1, nil, zap.NewExample(), rr, nil, "")
	srv := httptest.NewServer(tr.handler())
	defer srv.Close()

	body, err := marshalMessage(&raftpb.Message{Type: raftpb.MsgHeartbeat.Enum(), From: proto.Uint64(2), To: proto.Uint64(1)})
	require.NoError(t, err)

	post := func(method string, headers map[string]string, b []byte) int {
		req, err := httpgo.NewRequest(method, srv.URL+raftMessagePath, bytes.NewReader(b))
		require.NoError(t, err)
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := httpgo.DefaultClient.Do(req)
		require.NoError(t, err)
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	good := map[string]string{clusterIDHeader: clusterID, protocolHeader: protocolVersion}

	// Valid message → 204, and it reaches raft.
	require.Equal(t, httpgo.StatusNoContent, post(httpgo.MethodPost, good, body))
	require.Equal(t, 1, rr.processedCount())

	// Wrong cluster id and wrong protocol are both rejected, and never reach raft.
	require.Equal(t, httpgo.StatusPreconditionFailed,
		post(httpgo.MethodPost, map[string]string{clusterIDHeader: "someone-else", protocolHeader: protocolVersion}, body))
	require.Equal(t, httpgo.StatusPreconditionFailed,
		post(httpgo.MethodPost, map[string]string{clusterIDHeader: clusterID, protocolHeader: "99"}, body))
	require.Equal(t, httpgo.StatusMethodNotAllowed, post(httpgo.MethodGet, good, nil))
	require.Equal(t, 1, rr.processedCount(), "rejected requests must not reach raft")

	// A sender that has been removed from the cluster is forbidden — a removed
	// node must not be able to inject messages.
	rr.mu.Lock()
	rr.removed[2] = true
	rr.mu.Unlock()
	require.Equal(t, httpgo.StatusForbidden, post(httpgo.MethodPost, good, body))
	require.Equal(t, 1, rr.processedCount())
}

// TestHandler_Token covers the optional shared-secret guard: when a token is
// configured, only a request carrying the matching bearer is accepted. The
// token is injected into New directly — no process-environment mutation — which
// is the point of taking it as a parameter instead of reading COMMITTED_API_TOKEN
// inside the constructor.
func TestHandler_Token(t *testing.T) {
	rr := newRecordingRaft()
	tr := New(1, nil, zap.NewExample(), rr, nil, "s3cr3t")
	srv := httptest.NewServer(tr.handler())
	defer srv.Close()

	body, err := marshalMessage(&raftpb.Message{Type: raftpb.MsgHeartbeat.Enum(), From: proto.Uint64(2), To: proto.Uint64(1)})
	require.NoError(t, err)

	post := func(auth string) int {
		req, err := httpgo.NewRequest(httpgo.MethodPost, srv.URL+raftMessagePath, bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set(clusterIDHeader, clusterID)
		req.Header.Set(protocolHeader, protocolVersion)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := httpgo.DefaultClient.Do(req)
		require.NoError(t, err)
		_ = resp.Body.Close()
		return resp.StatusCode
	}

	require.Equal(t, httpgo.StatusUnauthorized, post(""))            // missing
	require.Equal(t, httpgo.StatusUnauthorized, post("Bearer nope")) // wrong
	require.Equal(t, httpgo.StatusNoContent, post("Bearer s3cr3t"))  // right
	require.Equal(t, 1, rr.processedCount())
}

// TestSend_RoundTrip exercises the full send path — Send → per-peer worker →
// POST → the peer's handler → its raft.Process — between two real transports,
// and that a MsgSnap's delivery is reported back as SnapshotFinish.
func TestSend_RoundTrip(t *testing.T) {
	rrB := newRecordingRaft()
	trB := New(2, nil, zap.NewExample(), rrB, nil, "")
	srvB := httptest.NewServer(trB.handler())
	defer srvB.Close()

	rrA := newRecordingRaft()
	trA := New(1, nil, zap.NewExample(), rrA, nil, "")
	defer trA.Stop()
	require.NoError(t, trA.AddPeer(raft.Peer{ID: 2, Context: []byte(srvB.URL)}))

	trA.Send([]*raftpb.Message{{Type: raftpb.MsgApp.Enum(), From: proto.Uint64(1), To: proto.Uint64(2)}})
	require.Eventually(t, func() bool { return rrB.processedCount() == 1 },
		2*time.Second, 10*time.Millisecond, "message should reach the peer's raft")

	trA.Send([]*raftpb.Message{{Type: raftpb.MsgSnap.Enum(), From: proto.Uint64(1), To: proto.Uint64(2)}})
	require.Eventually(t, func() bool {
		s, ok := rrA.snapStatus(2)
		return ok && s == raft.SnapshotFinish
	}, 2*time.Second, 10*time.Millisecond, "a delivered snapshot should report SnapshotFinish")
}

// TestSend_DeadPeerReportsUnreachable: a POST to a peer that isn't listening
// fails, and the failure is reported to raft so it backs off probing.
func TestSend_DeadPeerReportsUnreachable(t *testing.T) {
	dead := httptest.NewServer(httpgo.NotFoundHandler())
	url := dead.URL
	dead.Close() // now refuses connections — a fast failure

	rr := newRecordingRaft()
	tr := New(1, nil, zap.NewExample(), rr, nil, "")
	defer tr.Stop()
	require.NoError(t, tr.AddPeer(raft.Peer{ID: 9, Context: []byte(url)}))

	tr.Send([]*raftpb.Message{{Type: raftpb.MsgApp.Enum(), From: proto.Uint64(1), To: proto.Uint64(9)}})
	require.Eventually(t, func() bool { return rr.wasUnreachable(9) },
		3*time.Second, 10*time.Millisecond, "a failed send must report the peer unreachable")
}

// TestSend_NeverBlocks is the drop-under-backpressure property: Send must never
// block the raft loop even when a peer is dead and its queue fills.
func TestSend_NeverBlocks(t *testing.T) {
	dead := httptest.NewServer(httpgo.NotFoundHandler())
	url := dead.URL
	dead.Close()

	tr := New(1, nil, zap.NewExample(), newRecordingRaft(), nil, "")
	defer tr.Stop()
	require.NoError(t, tr.AddPeer(raft.Peer{ID: 9, Context: []byte(url)}))

	done := make(chan struct{})
	go func() {
		for range 100 * peerQueueDepth {
			tr.Send([]*raftpb.Message{{Type: raftpb.MsgApp.Enum(), From: proto.Uint64(1), To: proto.Uint64(9)}})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Send blocked under backpressure — it must drop, not block the raft loop")
	}
}
