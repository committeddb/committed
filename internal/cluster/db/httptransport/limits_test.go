package httptransport

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/db/wal"
	"github.com/committeddb/committed/pkg/segmentlog"
)

// The largest proposal a node accepts must cross every fixed bound on its way:
// a raft append carrying it to a follower, and a catch-up records part. These
// bounds are not derived from the proposal cap, so a raise of the event log's
// record limit has to raise them too; this test fails until it does.
func TestFixedBoundsCoverTheLargestEntry(t *testing.T) {
	max := ^uint64(0)
	entry := &raftpb.Entry{Term: &max, Index: &max, Type: raftpb.EntryNormal.Enum(), Data: make([]byte, wal.MaxProposalBytes)}
	msg := &raftpb.Message{
		Type: raftpb.MsgApp.Enum(), To: &max, From: &max, Term: &max, LogTerm: &max, Index: &max, Commit: &max,
		Entries: []*raftpb.Entry{entry},
	}
	require.LessOrEqual(t, proto.Size(msg), maxMessageBytes, "a raft append carrying the largest entry")
	// A records part is the serving node's 8 MiB budget plus the one record
	// that crosses it, each record a uvarint length and an 8-byte frame header.
	require.LessOrEqual(t, 8<<20+segmentlog.MaxRecordBytes+16, maxRecordsPartBytes, "a catch-up records part")
}
