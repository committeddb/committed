package wal

import (
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
)

// metadataSelection is shared by the legacy and experimental log scans. Callers
// feed entries in index order, capped at their scrub bound. It learns user kinds
// from registrations in that prefix, never from mutable live type state. Memory
// scales with registered types and selected keys; it is not an engine index.
type metadataSelection struct {
	latest   map[string]uint64
	userKind map[string]cluster.EntityKind
}

func newMetadataSelection() *metadataSelection {
	return &metadataSelection{latest: make(map[string]uint64), userKind: make(map[string]cluster.EntityKind)}
}

func (s *metadataSelection) observe(entry *pb.Entry) error {
	if entry.GetType() != pb.EntryNormal || entry.Data == nil {
		return nil
	}
	index := entry.GetIndex()
	return cluster.ForEachProposalEntity(entry.Data, func(typeID string, key, data []byte, isDelete bool) error {
		if cluster.IsType(typeID) && !isDelete {
			typ := new(cluster.Type)
			if err := typ.Unmarshal(data); err != nil {
				return err
			}
			s.userKind[typ.ID] = typ.EntityKind
			return nil
		}
		if !cluster.IsSystemTombstonable(typeID) && s.userKind[typeID] != cluster.EntityKindSnapshot {
			return nil
		}
		if key := string(tombstoneKey(typeID, key)); index > s.latest[key] {
			s.latest[key] = index
		}
		return nil
	})
}
