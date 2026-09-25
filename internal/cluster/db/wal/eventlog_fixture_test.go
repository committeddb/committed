package wal

import (
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterpb"
)

// Backend-neutral Committed records and type resolution shared by adapter tests.
func experimentEntry(t testing.TB, index uint64, kind pb.EntryType, entities ...*clusterpb.LogEntity) []byte {
	t.Helper()
	var data []byte
	if len(entities) > 0 {
		var err error
		data, err = proto.Marshal(&clusterpb.LogProposal{RequestID: 123, LogEntities: entities})
		if err != nil {
			t.Fatal(err)
		}
	}
	raw, err := proto.Marshal(&pb.Entry{Index: proto.Uint64(index), Term: proto.Uint64(3), Type: kind.Enum(), Data: data})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func experimentRow(key, value string) *clusterpb.LogEntity {
	return &clusterpb.LogEntity{Type: &clusterpb.TypeRef{ID: "items", Version: 1}, Body: &clusterpb.LogEntity_Row{Row: &clusterpb.LogRow{Key: []byte(key), Data: []byte(value)}}}
}

type eventTestResolver func(cluster.TypeRef) (*cluster.Type, error)

func (f eventTestResolver) ResolveType(ref cluster.TypeRef) (*cluster.Type, error) { return f(ref) }

func eventTestType(ref cluster.TypeRef) (*cluster.Type, error) {
	return &cluster.Type{ID: ref.ID, Version: ref.Version, Name: ref.ID}, nil
}
