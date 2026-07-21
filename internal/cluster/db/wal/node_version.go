package wal

import (
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/committeddb/committed/internal/cluster"
)

// handleNodeVersion persists a node's self-announced cluster feature level.
// Called from the apply path. The entity carries the node id in e.Key (8
// big-endian bytes) and the marshaled cluster.NodeVersion in e.Data; we store
// e.Data verbatim so the read methods can unmarshal it without re-wrapping.
//
// Last-writer-wins per node id: a node whose binary was upgraded simply
// re-announces a higher level and overwrites. The bucket rides along in
// snapshots (bbolt is serialized whole into CreateSnapshot), so the mapping is
// durable across restart, log compaction, and InstallSnapshot to a lagging
// follower.
func (s *Storage) handleNodeVersion(e *cluster.Entity, _ uint64) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(memberVersionBucket)
		if b == nil {
			return ErrBucketMissing
		}
		if err := b.Put(e.Key, e.Data); err != nil {
			return fmt.Errorf("[wal.node_version] put: %w", err)
		}
		return nil
	})
}

// MemberVersion returns the cluster feature level announced by node id, and
// whether one is known. A missing bucket or unknown id reports (0, false) — an
// un-announced node (one that hasn't announced yet, or a binary predating the
// mechanism) is a normal condition the caller treats conservatively as level 0.
func (s *Storage) MemberVersion(id uint64) (uint64, bool) {
	var (
		level uint64
		found bool
	)
	_ = s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(memberVersionBucket)
		if b == nil {
			return nil
		}
		raw := b.Get(cluster.NodeVersionKey(id))
		if raw == nil {
			return nil
		}
		n := &cluster.NodeVersion{}
		if err := n.Unmarshal(raw); err != nil {
			return nil
		}
		level = n.FeatureLevel
		found = true
		return nil
	})
	return level, found
}

// MemberVersions returns every known node id → announced cluster feature level.
// Used to compute the cluster-agreed minimum that gates semantically-skewed
// emission. A missing bucket yields an empty map.
func (s *Storage) MemberVersions() map[uint64]uint64 {
	out := make(map[uint64]uint64)
	_ = s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(memberVersionBucket)
		if b == nil {
			return nil
		}
		return b.ForEach(func(_, v []byte) error {
			n := &cluster.NodeVersion{}
			if err := n.Unmarshal(v); err != nil {
				return nil
			}
			out[n.NodeID] = n.FeatureLevel
			return nil
		})
	})
	return out
}

// DeleteMemberVersion drops the announced feature level for node id. Called from
// the membership-remove apply path (raft.applyConfChange) so a removed node's
// entry doesn't accumulate across add/remove churn. Idempotent: deleting an
// absent key is a no-op. The gate reads current members only, so a stale entry
// is harmless — this is hygiene, matching DeleteMemberAPIURL.
func (s *Storage) DeleteMemberVersion(id uint64) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(memberVersionBucket)
		if b == nil {
			return ErrBucketMissing
		}
		if err := b.Delete(cluster.NodeVersionKey(id)); err != nil {
			return fmt.Errorf("[wal.node_version] delete: %w", err)
		}
		return nil
	})
}
