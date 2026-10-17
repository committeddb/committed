package httptransport

import (
	"bytes"
	httpgo "net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
)

// identityRaft is a recordingRaft with a cluster identity.
type identityRaft struct {
	*recordingRaft
	mu        sync.Mutex
	id        cluster.ClusterID
	have      bool
	adoptable bool
	adopted   int
}

func (r *identityRaft) ClusterID() (cluster.ClusterID, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.id, r.have
}

func (r *identityRaft) AdoptClusterID(id cluster.ClusterID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.have {
		if r.id != id {
			return errMismatch
		}
		return nil
	}
	if !r.adoptable {
		return db.ErrNotAdoptable
	}
	r.id, r.have = id, true
	r.adopted++
	return nil
}

var errMismatch = errorString("belongs to another cluster")

type errorString string

func (e errorString) Error() string { return string(e) }

func mustID(t *testing.T) cluster.ClusterID {
	t.Helper()
	id, err := cluster.NewClusterID()
	require.NoError(t, err)
	return id
}

// postMessage POSTs a raft message for node 2 with the given identity headers.
func postMessage(t *testing.T, srv *httptest.Server, legacy, uuid, target, token string) int {
	t.Helper()
	to, from := uint64(2), uint64(1)
	data, err := marshalMessage(&raftpb.Message{Type: raftpb.MsgHeartbeat.Enum(), To: &to, From: &from})
	require.NoError(t, err)
	req, err := httpgo.NewRequest(httpgo.MethodPost, srv.URL+raftMessagePath, bytes.NewReader(data))
	require.NoError(t, err)
	req.Header.Set(protocolHeader, protocolVersion)
	if legacy != "" {
		req.Header.Set(clusterIDHeader, legacy)
	}
	if uuid != "" {
		req.Header.Set(clusterUUIDHeader, uuid)
	}
	if target != "" {
		req.Header.Set(targetNodeHeader, target)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := httpgo.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	return resp.StatusCode
}

func serve(t *testing.T, r db.TransportRaft, token string) *httptest.Server {
	t.Helper()
	tr := New(2, nil, zap.NewNop(), r, nil, nil, token)
	srv := httptest.NewServer(tr.handler())
	t.Cleanup(srv.Close)
	return srv
}

// The receive-side identity rules, endpoint by endpoint of the decision.
func TestAdmitPeer_ClusterID(t *testing.T) {
	mine, theirs := mustID(t), mustID(t)

	t.Run("same cluster is accepted", func(t *testing.T) {
		r := &identityRaft{recordingRaft: newRecordingRaft(), id: mine, have: true}
		srv := serve(t, r, "")
		require.Equal(t, httpgo.StatusNoContent, postMessage(t, srv, mine.String(), mine.String(), "2", ""))
		require.Len(t, r.processed, 1)
	})
	t.Run("another cluster is refused", func(t *testing.T) {
		r := &identityRaft{recordingRaft: newRecordingRaft(), id: mine, have: true}
		srv := serve(t, r, "")
		require.Equal(t, httpgo.StatusPreconditionFailed, postMessage(t, srv, theirs.String(), theirs.String(), "2", ""))
		require.Empty(t, r.processed)
	})
	t.Run("a node with an id refuses a sender without one", func(t *testing.T) {
		r := &identityRaft{recordingRaft: newRecordingRaft(), id: mine, have: true}
		srv := serve(t, r, "")
		require.Equal(t, httpgo.StatusPreconditionFailed, postMessage(t, srv, clusterID, "", "2", ""))
		require.Empty(t, r.processed)
	})
	t.Run("a fresh joiner adopts the id of the first message", func(t *testing.T) {
		r := &identityRaft{recordingRaft: newRecordingRaft(), adoptable: true}
		srv := serve(t, r, "")
		require.Equal(t, httpgo.StatusNoContent, postMessage(t, srv, mine.String(), mine.String(), "2", ""))
		got, ok := r.ClusterID()
		require.True(t, ok)
		require.Equal(t, mine, got)
		require.Equal(t, httpgo.StatusPreconditionFailed, postMessage(t, srv, theirs.String(), theirs.String(), "2", ""),
			"once adopted, another cluster is refused")
	})
	t.Run("a node without an id that isn't joining accepts but doesn't adopt", func(t *testing.T) {
		r := &identityRaft{recordingRaft: newRecordingRaft()} // the upgrade window
		srv := serve(t, r, "")
		require.Equal(t, httpgo.StatusNoContent, postMessage(t, srv, mine.String(), mine.String(), "2", ""))
		_, ok := r.ClusterID()
		require.False(t, ok)
		require.Zero(t, r.adopted)
	})
	t.Run("legacy peers without ids still talk", func(t *testing.T) {
		srv := serve(t, newRecordingRaft(), "")
		require.Equal(t, httpgo.StatusNoContent, postMessage(t, srv, clusterID, "", "", ""))
		require.Equal(t, httpgo.StatusPreconditionFailed, postMessage(t, srv, "some-other-constant", "", "", ""))
	})
}

// A request for another node is refused before the token is checked and
// before the body is read: a reused address never processes it.
func TestAdmitPeer_TargetNodeBeforeToken(t *testing.T) {
	r := newRecordingRaft()
	srv := serve(t, r, "secret")
	require.Equal(t, httpgo.StatusMisdirectedRequest, postMessage(t, srv, clusterID, "", "7", ""),
		"wrong target, no token: refused as misdirected, not unauthorized")
	require.Equal(t, httpgo.StatusUnauthorized, postMessage(t, srv, clusterID, "", "2", ""),
		"right target, no token: the token check still runs")
	require.Equal(t, httpgo.StatusNoContent, postMessage(t, srv, clusterID, "", strconv.Itoa(2), "secret"))
	require.Len(t, r.processed, 1)
}

// What a node sends: the legacy header carries the real id once the node has
// one (so an older release refuses it), plus the id and target headers.
func TestSetPeerHeaders(t *testing.T) {
	mine := mustID(t)
	h := httpgo.Header{}
	(&HttpTransport{raft: &identityRaft{recordingRaft: newRecordingRaft(), id: mine, have: true}}).setPeerHeaders(h, 3)
	require.Equal(t, mine.String(), h.Get(clusterIDHeader))
	require.Equal(t, mine.String(), h.Get(clusterUUIDHeader))
	require.Equal(t, "3", h.Get(targetNodeHeader))

	h = httpgo.Header{}
	(&HttpTransport{raft: newRecordingRaft()}).setPeerHeaders(h, 3)
	require.Equal(t, clusterID, h.Get(clusterIDHeader), "without an id, what older releases expect")
	require.Empty(t, h.Get(clusterUUIDHeader))
}
