package wal

import (
	"errors"

	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// actualFromLookup interprets one exact logical lookup. Replay includes metadata
// and has no streaming-reader applied watermark or system-type skip policy.
func actualFromLookup(log eventlog.Lookup, index uint64, resolver cluster.TypeResolver) (*cluster.Actual, error) {
	record, err := log.Read(index)
	if errors.Is(err, eventlog.ErrNotFound) {
		return nil, ErrActualNotFound
	}
	entry, err := decodeEventEntry(record, err)
	if err != nil {
		return nil, err
	}
	if entry.GetType() != pb.EntryNormal || entry.Data == nil {
		return nil, ErrActualNotFound
	}
	proposal := new(cluster.Proposal)
	if err := proposal.Unmarshal(entry.Data, resolver); err != nil {
		return nil, err
	}
	return &cluster.Actual{Index: entry.GetIndex(), Entities: proposal.Entities}, nil
}
