package db_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tlstransport "go.etcd.io/etcd/client/pkg/v3/transport"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster/db"
)

// recordingTransport is a db.Transport spy that records AddPeer calls and is
// otherwise inert, so a test can assert the startup reconcile connected the right
// peers without binding real listeners.
type recordingTransport struct {
	mu    sync.Mutex
	added []raft.Peer
	errC  chan error
}

func (r *recordingTransport) GetErrorC() chan error { return r.errC }
func (r *recordingTransport) Start(stopC <-chan struct{}) error {
	<-stopC
	return nil
}

func (r *recordingTransport) AddPeer(p raft.Peer) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.added = append(r.added, p)
	return nil
}
func (r *recordingTransport) RemovePeer(id uint64)        {}
func (r *recordingTransport) Send(msgs []*raftpb.Message) {}
func (r *recordingTransport) Stop()                       {}

func (r *recordingTransport) addedPeers() []raft.Peer {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]raft.Peer(nil), r.added...)
}

// TestReconcileTransport_ConnectsDurablePeerMissingFromSeed is the deterministic
// proof of the fix's reconcile step: a node whose durable membership records a
// peer (id 2) that the static COMMITTED_PEERS seed omits — the exact
// grew-then-restarted shape — connects that peer's transport at startup, without
// the peer ever appearing in the seed. reconcileTransportFromMembership runs
// synchronously inside NewRaft (before serveRaft), so no election or timing is
// involved.
func TestReconcileTransport_ConnectsDurablePeerMissingFromSeed(t *testing.T) {
	s := NewMemoryStorage()
	require.NoError(t, s.PutMemberPeerURL(2, []byte("http://n2:2380")))

	spy := &recordingTransport{errC: make(chan error, 1)}
	proposeC := make(chan []byte)
	confChangeC := make(chan *raftpb.ConfChangeV2)

	// Seed only self (id 1): node 2 was added dynamically after the seed was
	// written, so it is absent from COMMITTED_PEERS.
	ps := []raft.Peer{{ID: 1, Context: []byte("http://n1:2380")}}
	_, r := db.NewRaft(1, ps, s, proposeC, confChangeC,
		db.WithTickInterval(50*time.Millisecond),
		db.WithTransportFactory(func(_ uint64, _ []raft.Peer, _ *zap.Logger, _ db.TransportRaft, _ *tlstransport.TLSInfo, _ string) db.Transport {
			return spy
		}),
	)
	defer r.Close()

	added := spy.addedPeers()
	require.Len(t, added, 1, "the durable peer absent from the seed must be reconciled")
	require.Equal(t, uint64(2), added[0].ID)
	require.Equal(t, "http://n2:2380", string(added[0].Context))
}

// TestReconcileTransport_SkipsSelf proves reconcile never tries to dial this
// node's own id even if a stale self entry lingers in durable membership.
func TestReconcileTransport_SkipsSelf(t *testing.T) {
	s := NewMemoryStorage()
	require.NoError(t, s.PutMemberPeerURL(1, []byte("http://n1:2380"))) // self
	require.NoError(t, s.PutMemberPeerURL(2, []byte("http://n2:2380")))

	spy := &recordingTransport{errC: make(chan error, 1)}
	proposeC := make(chan []byte)
	confChangeC := make(chan *raftpb.ConfChangeV2)
	ps := []raft.Peer{{ID: 1, Context: []byte("http://n1:2380")}}
	_, r := db.NewRaft(1, ps, s, proposeC, confChangeC,
		db.WithTickInterval(50*time.Millisecond),
		db.WithTransportFactory(func(_ uint64, _ []raft.Peer, _ *zap.Logger, _ db.TransportRaft, _ *tlstransport.TLSInfo, _ string) db.Transport {
			return spy
		}),
	)
	defer r.Close()

	for _, p := range spy.addedPeers() {
		require.NotEqual(t, uint64(1), p.ID, "reconcile must skip self")
	}
}
