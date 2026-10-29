package httptransport

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster/db"
)

// memberRaft is a recordingRaft that answers MembershipView.
type memberRaft struct {
	*recordingRaft
	members map[uint64]bool
}

func (r *memberRaft) IsMember(id uint64) bool { return r.members[id] }

func newSelfReportTransport(t *testing.T) *HttpTransport {
	t.Helper()
	tr := New(1, nil, zap.NewNop(), &memberRaft{recordingRaft: newRecordingRaft(), members: map[uint64]bool{2: true, 3: true}}, nil, nil, "")
	t.Cleanup(tr.Stop)
	return tr
}

func addressOf(t *testing.T, tr *HttpTransport, id uint64) db.PeerAddressView {
	t.Helper()
	v, ok := tr.PeerAddress(id)
	require.True(t, ok)
	return v
}

// A self-report replaces a peer's address only when the current one is
// failing: a working address — or one not yet tried — is kept, so a node that
// advertises an address its peers can't reach is never cut off.
func TestSelfReport_ReplacesOnlyAFailingAddress(t *testing.T) {
	tr := newSelfReportTransport(t)
	require.NoError(t, tr.AddPeer(raft.Peer{ID: 2, Context: []byte("http://old:9022")}))

	tr.observeSelfReport(2, "http://new:9022")
	require.Equal(t, "http://old:9022", addressOf(t, tr, 2).URL, "an untried address is kept")

	pr := tr.peers[2]
	pr.health.succeeded()
	tr.observeSelfReport(2, "http://new:9022")
	require.Equal(t, "http://old:9022", addressOf(t, tr, 2).URL, "a working address is kept")

	time.Sleep(time.Millisecond)
	pr.health.lastFailure.Store(time.Now().UnixNano())
	tr.observeSelfReport(2, "http://new:9022/")
	v := addressOf(t, tr, 2)
	require.Equal(t, "http://new:9022", v.URL, "a failing address is replaced by the reported one")
	require.Equal(t, db.PeerAddressFromSelfReport, v.Source)
	require.False(t, v.Delivered, "nothing delivered to the new address yet")
}

// Only a current member's self-report counts, and an address no peer can
// dial is never taken.
func TestSelfReport_MembersOnly(t *testing.T) {
	tr := newSelfReportTransport(t)
	tr.observeSelfReport(9, "http://stranger:9022")
	_, ok := tr.PeerAddress(9)
	require.False(t, ok, "a non-member can't register itself")

	tr.observeSelfReport(3, "http://0.0.0.0:9022")
	_, ok = tr.PeerAddress(3)
	require.False(t, ok, "a wildcard address is never taken")

	tr.observeSelfReport(3, "http://n3:9022")
	require.Equal(t, "http://n3:9022", addressOf(t, tr, 3).URL, "a member with no entry gets one")
}

// The transport sends its own advertised URL, and never a wildcard.
func TestSelfReport_SenderHeader(t *testing.T) {
	tr := New(1, []raft.Peer{{ID: 1, Context: []byte("http://N1:9022/")}}, zap.NewNop(), newRecordingRaft(), nil, nil, "")
	h := map[string][]string{}
	tr.setPeerHeaders(h, 2)
	require.Equal(t, []string{"http://n1:9022"}, h[senderURLHeader])

	wild := New(1, []raft.Peer{{ID: 1, Context: []byte("http://0.0.0.0:9022")}}, zap.NewNop(), newRecordingRaft(), nil, nil, "")
	h = map[string][]string{}
	wild.setPeerHeaders(h, 2)
	require.Empty(t, h[senderURLHeader])
}
