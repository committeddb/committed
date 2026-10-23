package db_test

import (
	"context"
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
}

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

func (r *recordingTransport) SetPeerAddress(id uint64, u, _ string) error {
	return r.AddPeer(raft.Peer{ID: id, Context: []byte(u)})
}

func (r *recordingTransport) PeerAddress(uint64) (db.PeerAddressView, bool) {
	return db.PeerAddressView{}, false
}
func (r *recordingTransport) RemovePeer(id uint64)        {}
func (r *recordingTransport) Send(msgs []*raftpb.Message) {}
func (r *recordingTransport) Stop()                       {}
func (r *recordingTransport) FetchEvents(context.Context, db.EventFetchRequest, db.EventSink) (db.EventFetchResult, error) {
	return db.EventFetchResult{}, db.ErrNoPeerToFetchFrom
}

// startWithSeeds starts a Raft on s with COMMITTED_PEERS ps and returns the
// transport seeds the node chose (choosePeerAddresses) as id → address.
func startWithSeeds(t *testing.T, s interface {
	db.ConsensusStorage
	db.MembershipStorage
}, ps []raft.Peer,
) map[uint64]string {
	t.Helper()
	var seeds []raft.Peer
	proposeC := make(chan []byte)
	confChangeC := make(chan *raftpb.ConfChangeV2)
	_, r := db.NewRaft(1, ps, s, proposeC, confChangeC,
		db.WithTickInterval(50*time.Millisecond),
		db.WithTransportFactory(func(_ uint64, peers []raft.Peer, _ *zap.Logger, _ db.TransportRaft, _ db.EventServer, _ *tlstransport.TLSInfo, _ string) db.Transport {
			seeds = peers
			return &recordingTransport{}
		}),
	)
	r.Close()
	out := make(map[uint64]string, len(seeds))
	for _, p := range seeds {
		out[p.ID] = string(p.Context)
	}
	return out
}

// A member recorded in durable membership but absent from COMMITTED_PEERS —
// the grew-then-restarted shape — is connected at startup.
func TestPeerAddresses_RecordedMemberMissingFromEnv(t *testing.T) {
	s := NewMemoryStorage()
	require.NoError(t, s.PutMemberPeerURL(2, []byte("http://n2:2380")))
	seeds := startWithSeeds(t, s, []raft.Peer{{ID: 1, Context: []byte("http://n1:2380")}})
	require.Equal(t, "http://n2:2380", seeds[2])
	require.Equal(t, "http://n1:2380", seeds[1], "self is seeded with its own listen address")
}

// A stale self entry in durable membership never replaces this node's own
// listen address from COMMITTED_PEERS.
func TestPeerAddresses_SkipsRecordedSelf(t *testing.T) {
	s := NewMemoryStorage()
	require.NoError(t, s.PutMemberPeerURL(1, []byte("http://stale:2380")))
	require.NoError(t, s.PutMemberPeerURL(2, []byte("http://n2:2380")))
	seeds := startWithSeeds(t, s, []raft.Peer{{ID: 1, Context: []byte("http://n1:2380")}})
	require.Equal(t, "http://n1:2380", seeds[1])
}

// bootStorage adds the node-local last-boot record to the in-memory storage.
type bootStorage struct {
	*MemoryStorage
	last map[uint64]string
	have bool
}

func (b *bootStorage) PeersAtLastBoot() (map[uint64]string, bool, error) {
	return b.last, b.have, nil
}

func (b *bootStorage) SetPeersAtLastBoot(p map[uint64]string) error {
	b.last, b.have = p, true
	return nil
}

func env(peers map[uint64]string) []raft.Peer {
	out := make([]raft.Peer, 0, len(peers))
	for id, u := range peers {
		out = append(out, raft.Peer{ID: id, Context: []byte(u)})
	}
	return out
}

// The boot-time order of authority for a peer's address (choosePeerAddresses):
// a COMMITTED_PEERS entry the operator edited since the last boot wins (and is
// recorded); an unedited one never overrides the recorded address; the first
// boot without a last-boot record keeps the recorded address.
func TestPeerAddresses_Precedence(t *testing.T) {
	old := map[uint64]string{1: "http://n1:9022", 2: "http://n2:9022", 3: "http://n3:9022"}
	moved := map[uint64]string{1: "http://n1:9022", 2: "http://m2:9022", 3: "http://m3:9022"}

	t.Run("edited since the last boot: env wins and is recorded", func(t *testing.T) {
		s := &bootStorage{MemoryStorage: NewMemoryStorage(), last: old, have: true}
		require.NoError(t, s.PutMemberPeerURL(2, []byte(old[2])))
		require.NoError(t, s.PutMemberPeerURL(3, []byte(old[3])))
		seeds := startWithSeeds(t, s, env(moved))
		require.Equal(t, moved[2], seeds[2])
		require.Equal(t, moved[3], seeds[3])
		require.Equal(t, moved[2], s.MemberPeerURLs()[2], "the next boot keeps it")
		require.Equal(t, moved, s.last, "this boot's env is recorded")
	})

	t.Run("unedited stale env never overrides the recorded address", func(t *testing.T) {
		s := &bootStorage{MemoryStorage: NewMemoryStorage(), last: old, have: true}
		require.NoError(t, s.PutMemberPeerURL(2, []byte(moved[2]))) // the cluster agreed node 2 moved
		require.NoError(t, s.PutMemberPeerURL(3, []byte(old[3])))
		seeds := startWithSeeds(t, s, env(old))
		require.Equal(t, moved[2], seeds[2])
	})

	t.Run("first boot with this rule keeps the recorded address", func(t *testing.T) {
		s := &bootStorage{MemoryStorage: NewMemoryStorage()}
		require.NoError(t, s.PutMemberPeerURL(2, []byte(old[2])))
		seeds := startWithSeeds(t, s, env(moved))
		require.Equal(t, old[2], seeds[2])
		require.Equal(t, moved[3], seeds[3], "env fills a member with no record")
		require.True(t, s.have, "and the env is recorded for next time")
	})
}
