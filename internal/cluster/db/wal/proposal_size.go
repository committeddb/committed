package wal

import (
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/pkg/segmentlog"
)

// MaxProposalBytes is the largest marshaled proposal the event log can store:
// the raft entry carrying it (the proposal plus term, index and type) must fit
// one record, segmentlog.MaxRecordBytes. A proposal cap above it would admit
// entries every node fails to apply.
var MaxProposalBytes = maxProposalBytes(segmentlog.MaxRecordBytes)

// maxProposalBytes is the largest data length whose entry fits in limit,
// sized at the largest term and index an entry can carry.
func maxProposalBytes(limit int) uint64 {
	empty := proto.Size(&pb.Entry{Term: proto.Uint64(^uint64(0)), Index: proto.Uint64(^uint64(0)), Type: pb.EntryConfChangeV2.Enum(), Data: []byte{}})
	entry := func(n int) int {
		return empty - protowire.SizeVarint(0) + protowire.SizeVarint(uint64(n)) + n //nolint:gosec // G115: n is a non-negative record size
	}
	n := limit - (entry(limit) - limit)
	for n > 0 && entry(n) > limit {
		n--
	}
	return uint64(n) //nolint:gosec // G115: n is a non-negative record size
}
