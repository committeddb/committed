package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
)

// TestApplyRejectsNonRowNonDeleteOnInternalType pins the dispatch-boundary
// variant guard: every internal (config/coordination) handler understands
// exactly a row and its delete tombstone, so a refresh-boundary marker —
// or any future variant — bearing an internal type ID must FAIL the apply
// loudly. Before the guard it silently "applied": handleSyncable read the
// marker's nil Data as an empty config and degraded it into a junk
// config-error record instead of surfacing a malformed entry.
func TestApplyRejectsNonRowNonDeleteOnInternalType(t *testing.T) {
	s := NewStorageWithParser(t, nil, parser.New())
	defer s.Cleanup()

	// Borrow the syncable system type from a real config entity, then dress a
	// refresh marker in it — the malformed shape the guard must reject.
	seed, err := cluster.NewUpsertSyncableEntity(&cluster.Configuration{
		ID: "any", MimeType: "text/toml", Data: []byte("[syncable]\nname = \"any\""),
	})
	require.NoError(t, err)
	marker := cluster.NewRefreshBoundaryEntity(seed.Type, 1)

	bs, err := (&cluster.Proposal{Entities: []*cluster.Entity{marker}}).Marshal()
	require.NoError(t, err)
	term, index := uint64(1), uint64(1)
	ent := &pb.Entry{Term: &term, Index: &index, Type: pb.EntryNormal.Enum(), Data: bs}
	require.NoError(t, s.Save(&defaultHardState, []*pb.Entry{ent}, &defaultSnap))

	err = s.ApplyCommitted(ent)
	require.Error(t, err,
		"a refresh marker on an internal type is malformed and must fail the apply, not silently degrade")
	require.Contains(t, err.Error(), "not applicable to internal type")
}
