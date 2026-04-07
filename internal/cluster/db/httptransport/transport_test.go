package httptransport

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

// fakeRaft is a minimal Raft implementation used to construct an HttpTransport
// without spinning up an actual cluster.
type fakeRaft struct{}

func (f *fakeRaft) Process(ctx context.Context, m raftpb.Message) error { return nil }
func (f *fakeRaft) IsIDRemoved(id uint64) bool                          { return false }
func (f *fakeRaft) ReportUnreachable(id uint64)                         {}
func (f *fakeRaft) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}

// TestNew_CreatesTransport verifies that the constructor returns a non-nil
// transport with an accessible error channel.
func TestNew_CreatesTransport(t *testing.T) {
	peers := []raft.Peer{
		{ID: 1, Context: []byte("http://127.0.0.1:12379")},
	}
	tr := New(1, peers, zap.NewExample(), &fakeRaft{})
	require.NotNil(t, tr)
	require.NotNil(t, tr.GetErrorC())
}

// TestAddPeer_InvalidURL verifies that AddPeer returns an error when given
// a peer context that is not a valid URL.
func TestAddPeer_InvalidURL(t *testing.T) {
	peers := []raft.Peer{
		{ID: 1, Context: []byte("http://127.0.0.1:12379")},
	}
	tr := New(1, peers, zap.NewExample(), &fakeRaft{})

	err := tr.AddPeer(raft.Peer{ID: 2, Context: []byte("://bad-url")})
	require.NotNil(t, err)
}
