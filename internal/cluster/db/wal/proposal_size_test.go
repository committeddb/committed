package wal_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/wal"
	"github.com/committeddb/committed/pkg/segmentlog"
)

// proposalOfSize is a proposal whose marshaled bytes — what the cap measures
// and raft replicates — are exactly size long.
func proposalOfSize(t *testing.T, size int) []byte {
	t.Helper()
	pad := size
	for range 8 {
		e := cluster.NewUpsertEntity(&cluster.Type{ID: "bulk"}, []byte("k"), bytes.Repeat([]byte("x"), pad))
		bs, err := (&cluster.Proposal{Entities: []*cluster.Entity{e}}).Marshal()
		require.NoError(t, err)
		if len(bs) == size {
			return bs
		}
		pad -= len(bs) - size
	}
	t.Fatalf("no proposal of exactly %d bytes", size)
	return nil
}

// A proposal the cap admits is one the event log stores: an entry carrying a
// proposal exactly at the default cap, or at the largest cap a node accepts,
// applies.
func TestApplyStoresAProposalAtTheCap(t *testing.T) {
	for _, size := range []uint64{db.DefaultMaxProposalBytes, wal.MaxProposalBytes} {
		s := NewStorage(t, nil)
		saveEntity(t, userType(t, "bulk", cluster.EntityKindRevision), s, 1, 1)
		bs := proposalOfSize(t, int(size))
		term, index := uint64(1), uint64(2)
		entry := &pb.Entry{Term: &term, Index: &index, Type: pb.EntryNormal.Enum(), Data: bs}
		require.NoError(t, s.Save(&pb.HardState{Term: &term, Commit: &index}, []*pb.Entry{entry}, &defaultSnap))
		require.NoError(t, s.ApplyCommitted(entry), "a %d-byte proposal", size)
		require.Equal(t, index, s.EventIndex())
		s.Cleanup()
	}
}

// The largest proposal storage accepts is exactly the one whose entry, at any
// term and index, fills a record.
func TestMaxProposalBytesFillsARecord(t *testing.T) {
	limit := segmentlog.MaxRecordBytes
	n := int(wal.MaxProposalBytes)
	size := func(data int) int {
		return proto.Size(&pb.Entry{Term: proto.Uint64(^uint64(0)), Index: proto.Uint64(^uint64(0)), Type: pb.EntryConfChangeV2.Enum(), Data: make([]byte, data)})
	}
	require.LessOrEqual(t, size(n), limit)
	require.Greater(t, size(n+1), limit)
	require.Equal(t, uint64(67108835), wal.MaxProposalBytes,
		"docs/operations/http-limits.md and e2e/multinode state this number")
}
