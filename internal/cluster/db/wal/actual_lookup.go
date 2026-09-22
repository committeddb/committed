package wal

import (
	"errors"

	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// exactEntry positions a private cursor and preserves its decoded search result.
// SeekGE may select a later survivor, which is absence for an exact lookup.
// The caller owns the cursor and its read lifetime.
func exactEntry(cursor entryCursor, index uint64) (*pb.Entry, error) {
	if err := cursor.SeekGE(index); err != nil {
		return nil, err
	}
	entry, err := cursor.Current()
	if errors.Is(err, eventlog.ErrNotFound) {
		return nil, ErrActualNotFound
	}
	if err != nil {
		return nil, err
	}
	if entry.GetIndex() != index {
		return nil, ErrActualNotFound
	}
	return entry, nil
}

// actualFromEntry includes metadata and has no streaming-reader type skip policy.
func actualFromEntry(entry *pb.Entry, resolver cluster.TypeResolver) (*cluster.Actual, error) {
	if entry.GetType() != pb.EntryNormal || entry.Data == nil {
		return nil, ErrActualNotFound
	}
	proposal := new(cluster.Proposal)
	if err := proposal.Unmarshal(entry.Data, resolver); err != nil {
		return nil, err
	}
	return &cluster.Actual{Index: entry.GetIndex(), Entities: proposal.Entities}, nil
}
